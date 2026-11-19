// Copyright 2026 Mozilla Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Cargo rlib compiles mapped for REAPI.

use super::*;
use crate::compiler::remote::{LocalArg, LocalCompileChanges};
use crate::reapi::{
    ActionEnv, CompileLocallyReason, NotEligible, PathMapper, ReapiCompile, ToolchainKey,
    WorkerProgram, lexically_normalize, sysroot_copy, traversed_directories,
};
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) fn map_for_reapi(
    compilation: &RustCompilation,
) -> std::result::Result<ReapiCompile, NotEligible> {
    // PathMapper doesn't convert Windows paths to the /-separated paths REAPI uses.
    if cfg!(windows) {
        return Err(CompileLocallyReason::WindowsPaths.into());
    }
    // A driver such as clippy-driver prints rustc's commit hash, so its action would share
    // results with rustc's, without the driver's lints.
    if compilation
        .executable
        .file_stem()
        .is_none_or(|stem| stem != "rustc")
    {
        return Err(NotEligible::new(
            CompileLocallyReason::RustcDriver,
            anyhow!("{}", compilation.executable.display()),
        ));
    }
    // Without a commit hash, an updated rustc in the same sysroot can't be told apart
    // from the old one.
    let Some(commit_hash) = compilation.commit_hash.clone() else {
        return Err(CompileLocallyReason::NoCommitHash.into());
    };
    mapped_compile(compilation, path_mapper(compilation)?, commit_hash)
}

/// The commit hash in `rustc -vV` output, or `None` for a rustc built without git
/// information, which prints `unknown`.
pub(super) fn parse_commit_hash(version: &str) -> Option<String> {
    version
        .lines()
        .find_map(|line| line.strip_prefix("commit-hash: "))
        .filter(|hash| *hash != "unknown")
        .map(str::to_owned)
}

/// Arguments that make `compilation`'s local compile write the same outputs as a
/// worker's compile, and a mapper for its stdout and stderr. `None` on Windows, for a
/// rustc driver's compile, for a compile that has no `PathMapper` or remaps paths itself,
/// or when the sysroot copy can't be made. May make a sysroot copy, so call it from a
/// blocking thread.
pub(super) fn reapi_local_compile_changes(
    compilation: &RustCompilation,
    sysroot_copies_dir: &Path,
) -> Option<LocalCompileChanges> {
    // A driver's compile never runs through REAPI, and neither do its dependents'.
    let is_rustc = compilation
        .executable
        .file_stem()
        .is_some_and(|stem| stem == "rustc");
    // The appended mappings would override the compile's own.
    let remaps_paths = compilation
        .arguments
        .iter()
        .any(|argument| argument.flag_str() == Some("--remap-path-prefix"));
    if cfg!(windows) || !is_rustc || remaps_paths {
        return None;
    }
    let path_mapper = path_mapper(compilation).ok()?;
    let sysroot = match sysroot_copy(&compilation.sysroot, sysroot_copies_dir) {
        Ok(sysroot) => sysroot,
        Err(e) => {
            // A failure repeats on every compile, so warn once.
            static WARNED: AtomicBool = AtomicBool::new(false);
            if WARNED.swap(true, Ordering::Relaxed) {
                debug!("REAPI: failed to copy the sysroot without rust-src: {e:#}");
            } else {
                warn!(
                    "REAPI: failed to copy the sysroot without rust-src, so local compiles won't write the same outputs as REAPI workers: {e:#}"
                );
            }
            return None;
        }
    };
    let mut args: Vec<LocalArg> = path_mapper
        .input_root_remap_args()
        .into_iter()
        .map(|arg| LocalArg {
            hashed_as: arg.clone(),
            arg,
        })
        .collect();
    let mut sysroot_arg = OsString::from("--sysroot=");
    sysroot_arg.push(sysroot);
    // The sysroot copy's path doesn't change rlib outputs.
    args.push(LocalArg {
        arg: sysroot_arg,
        hashed_as: "--sysroot=<sysroot copy>".into(),
    });
    Some(LocalCompileChanges {
        args,
        stdout_stderr_mapper: Some(Box::new(move |text| {
            path_mapper.input_root_paths_to_local(text)
        })),
        runs_remote_command: false,
    })
}

/// `dir` without its trailing `<profile>/deps`, or `None` if it doesn't end in `deps`.
pub(super) fn deps_dir_base(dir: &Path) -> Option<&Path> {
    if dir.file_name()? != "deps" {
        return None;
    }
    dir.ancestors().nth(2)
}

/// The compile's `PathMapper`, whose build root is the common ancestor of the
/// [`deps_dir_base`] of the output dir and of each -L dir that has one.
fn path_mapper(compilation: &RustCompilation) -> std::result::Result<PathMapper, NotEligible> {
    let cwd = &compilation.cwd;
    let output_dir = compilation
        .outputs
        .values()
        .next()
        .and_then(|output| {
            lexically_normalize(&cwd.join(&output.path))
                .parent()
                .map(Path::to_owned)
        })
        .ok_or_else(|| {
            NotEligible::new(
                CompileLocallyReason::UnexpectedCargoLayout,
                anyhow!("no outputs"),
            )
        })?;
    let Some(build_root) = deps_dir_base(&output_dir) else {
        // A build script that runs rustc itself writes into its OUT_DIR, mostly to probe
        // which features rustc supports. Many probes fail on purpose, and a failed compile
        // would run on a worker twice before running locally.
        let in_out_dir = compilation.env_vars.iter().any(|(name, dir)| {
            name == "OUT_DIR" && output_dir.starts_with(lexically_normalize(&cwd.join(dir)))
        });
        return Err(if in_out_dir {
            NotEligible::new(
                CompileLocallyReason::BuildScriptRustc,
                anyhow!("{} is in OUT_DIR", output_dir.display()),
            )
        } else {
            NotEligible::new(
                CompileLocallyReason::UnexpectedCargoLayout,
                anyhow!("{} isn't a <profile>/deps dir", output_dir.display()),
            )
        });
    };
    let mut build_root = build_root.to_owned();
    // -L dirs not ending in deps, such as a build script's OUT_DIR, are mapped with the
    // other arguments; one outside both roots compiles locally.
    let link_dirs: Vec<PathBuf> = compilation
        .crate_link_paths
        .iter()
        .map(|dir| lexically_normalize(&cwd.join(dir)))
        .collect();
    for link_dir_base in link_dirs.iter().filter_map(|dir| deps_dir_base(dir)) {
        build_root = build_root
            .components()
            .zip(link_dir_base.components())
            .take_while(|(a, b)| a == b)
            .map(|(a, _)| a)
            .collect();
    }
    // Rejects a build root holding the cwd, such as `/`, since the cwd's paths would
    // then map under BUILD_ROOT_DIR and depend on where the checkout lives.
    if cwd.starts_with(&build_root) {
        return Err(NotEligible::new(
            CompileLocallyReason::UnexpectedCargoLayout,
            anyhow!("build root {} holds the cwd", build_root.display()),
        ));
    }
    Ok(PathMapper::new(cwd.clone(), build_root))
}

fn mapped_compile(
    compilation: &RustCompilation,
    path_mapper: PathMapper,
    commit_hash: String,
) -> std::result::Result<ReapiCompile, NotEligible> {
    // A staticlib bundles the object code of every dependency and of std, which the
    // input root doesn't hold.
    if compilation.crate_types.staticlib {
        return Err(CompileLocallyReason::Staticlib.into());
    }
    let cwd = &compilation.cwd;
    let absolute_inputs: Vec<PathBuf> = compilation.inputs.iter().map(|p| cwd.join(p)).collect();

    let mut arguments = vec![];
    let mut mapped_crate_root = None;
    let mut target_triple = None;
    for argument in &compilation.arguments {
        if let Argument::Raw(crate_root) = argument {
            let mapped = path_mapper
                .to_input_root(Path::new(crate_root))
                .ok_or_else(|| {
                    NotEligible::new(
                        CompileLocallyReason::PathOutsideInputRoot,
                        anyhow!("crate root {crate_root:?}"),
                    )
                })?;
            if Path::new(crate_root).is_absolute() {
                mapped_crate_root = Some(mapped);
            } else {
                arguments.push(mapped);
            }
            continue;
        }
        // Cargo passes the terminal's width only when stderr is a terminal, so sending it
        // would give each width its own action. It only changes how diagnostics wrap.
        if argument.flag_str() == Some("--diagnostic-width") {
            continue;
        }
        // A compile's --remap-path-prefix names local paths, so a worker's output would
        // differ from a local compile's.
        if argument.flag_str() == Some("--remap-path-prefix") {
            return Err(NotEligible::new(
                CompileLocallyReason::RemapPathPrefix,
                anyhow!("{argument:?}"),
            ));
        }
        match argument.get_data() {
            Some(Target(ArgTarget::Name(name))) => target_triple = Some(name.clone()),
            Some(Target(_)) => {
                return Err(NotEligible::new(
                    CompileLocallyReason::CustomTargetSpec,
                    anyhow!("{argument:?}"),
                ));
            }
            // A worker would build for its own CPU, whose features the client machine may lack.
            Some(CodeGen(ArgCodegen {
                opt,
                value: Some(value),
            })) if opt == "target-cpu" && value == "native" => {
                return Err(NotEligible::new(
                    CompileLocallyReason::NativeCpu,
                    anyhow!("{argument:?}"),
                ));
            }
            Some(Unstable(ArgUnstable {
                opt,
                value: Some(value),
            })) if opt == "tune-cpu" && value == "native" => {
                return Err(NotEligible::new(
                    CompileLocallyReason::NativeCpu,
                    anyhow!("{argument:?}"),
                ));
            }
            // The PGO profile isn't mapped or added to the input root.
            Some(CodeGen(ArgCodegen { opt, .. }))
                if opt == "profile-use" || opt == "profile-sample-use" =>
            {
                return Err(NotEligible::new(
                    CompileLocallyReason::ProfileUse,
                    anyhow!("{argument:?}"),
                ));
            }
            Some(Unstable(ArgUnstable { opt, .. })) if opt == "profile-sample-use" => {
                return Err(NotEligible::new(
                    CompileLocallyReason::ProfileUse,
                    anyhow!("{argument:?}"),
                ));
            }
            // rustc reads an option's path on the worker, which doesn't have the file. An
            // rlib compile doesn't link, so it never reads the linker options' paths.
            Some(CodeGen(ArgCodegen {
                opt,
                value: Some(value),
            }))
            | Some(Unstable(ArgUnstable {
                opt,
                value: Some(value),
            })) if Path::new(value).is_absolute()
                && !matches!(opt.as_str(), "linker" | "link-arg" | "link-args") =>
            {
                return Err(NotEligible::new(
                    CompileLocallyReason::AbsolutePathInOption,
                    anyhow!("{argument:?}"),
                ));
            }
            _ => {}
        }
        for arg in argument.iter_strings(|p: &Path| path_mapper.to_input_root(p)) {
            arguments.push(arg.map_err(|e| {
                NotEligible::new(
                    CompileLocallyReason::PathOutsideInputRoot,
                    anyhow!("argument {argument:?}: {e}"),
                )
            })?);
        }
    }
    if let Some(path) = absolute_inputs
        .iter()
        .find(|p| path_mapper.to_input_root(p).is_none())
    {
        return Err(NotEligible::new(
            CompileLocallyReason::SourceOutsideInputRoot,
            anyhow!("{}", path.display()),
        ));
    }
    // A --target value may name a JSON spec in RUST_TARGET_PATH, which isn't in the input root.
    if compilation
        .env_vars
        .iter()
        .any(|(name, _)| name == "RUST_TARGET_PATH")
    {
        return Err(NotEligible::new(
            CompileLocallyReason::CustomTargetSpec,
            anyhow!("RUST_TARGET_PATH is set"),
        ));
    }
    // A worker may have another host triple, so the target triple is always passed.
    let target_triple = match target_triple {
        Some(target_triple) => target_triple,
        None => {
            arguments.push(format!("--target={}", compilation.host));
            compilation.host.clone()
        }
    };

    let env = action_env(compilation, &path_mapper, &target_triple)?;

    let mut input_root_files = absolute_inputs;
    input_root_files.extend(dependency_files(compilation, &path_mapper)?);
    input_root_files.sort();
    input_root_files.dedup();
    let input_root_directories = traversed_directories(&input_root_files, &path_mapper)?;
    let outputs = compilation
        .outputs
        .values()
        .map(|output| cwd.join(&output.path))
        .collect();
    Ok(ReapiCompile {
        path_mapper,
        arguments,
        program: WorkerProgram::Rustc { mapped_crate_root },
        toolchain: ToolchainKey::rustc(commit_hash, compilation.sysroot.clone(), target_triple),
        env,
        input_root_files,
        input_root_directories,
        preprocessed_source: None,
        outputs,
    })
}

/// The files of the compile's dependencies that rustc reads: the externs, the libraries of
/// the crates in their graphs, and those crates' sources.
fn dependency_files(
    compilation: &RustCompilation,
    path_mapper: &PathMapper,
) -> std::result::Result<Vec<PathBuf>, NotEligible> {
    let cwd = &compilation.cwd;
    let externs: Vec<PathBuf> = compilation
        .arguments
        .iter()
        .filter_map(|argument| match argument.get_data() {
            Some(Extern(ArgExtern { path, .. })) => Some(cwd.join(path)),
            _ => None,
        })
        .collect();
    // -Z ls lists every crate in an extern's graph. std's crates aren't in the -L dirs, so
    // the lookup below skips them.
    let mut dep_stems = HashSet::new();
    for extern_lib in externs
        .iter()
        .filter(|p| has_extension(p, &[RLIB_EXTENSION, RMETA_EXTENSION]))
    {
        let reader = compilation.rlib_dep_reader.as_ref().ok_or_else(|| {
            NotEligible::new(
                CompileLocallyReason::UnreadableDependencies,
                anyhow!("no rlib dependency reader"),
            )
        })?;
        dep_stems.extend(
            reader
                .discover_rlib_dep_stems(&compilation.env_vars, extern_lib)
                .map_err(|e| NotEligible::new(CompileLocallyReason::UnreadableDependencies, e))?,
        );
    }
    // Only the crates in the externs' graphs go into the input root, so it doesn't
    // change with what else the -L dirs hold.
    let mut dep_libs = externs;
    for dir in &compilation.crate_link_paths {
        // rmeta or rlib first, since a crate that is also a cdylib has a dylib without
        // metadata; proc macros have only a dylib.
        dep_libs.extend(dep_stems.iter().filter_map(|stem| {
            [RMETA_EXTENSION, RLIB_EXTENSION, DLL_EXTENSION]
                .iter()
                .map(|ext| dir.join(format!("{RLIB_PREFIX}{stem}.{ext}")))
                .find(|lib| lib.is_file())
        }));
    }
    // rustc 1.94+ (rust-lang/rust#149709) records in its output whether a
    // dependency's relative source files exist with the same contents, so the
    // dependencies' sources under the cwd or build root go into the input root.
    let mut dep_sources = vec![];
    let mut dep_infos_read = HashSet::new();
    for lib in &dep_libs {
        let Some(stem) = lib
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.strip_prefix(RLIB_PREFIX))
            .filter(|_| has_extension(lib, &[RLIB_EXTENSION, RMETA_EXTENSION, DLL_EXTENSION]))
        else {
            continue;
        };
        let dep_info = lib.with_file_name(format!("{stem}.d"));
        if !dep_infos_read.insert(dep_info.clone()) {
            continue;
        }
        let dep_info_text = fs::read_to_string(&dep_info).map_err(|e| {
            NotEligible::new(CompileLocallyReason::UnreadableDependencies, e.into())
        })?;
        // rustc writes the paths as cargo passes them: relative to the workspace root, the
        // cwd of every workspace member's compile, or absolute for other packages.
        let (inside, outside): (Vec<_>, Vec<_>) = parse_dep_info(&dep_info_text, cwd)
            .into_iter()
            .partition(|p| path_mapper.to_input_root(p).is_some());
        if !outside.is_empty() {
            debug!(
                "[{}]: {stem}'s sources outside the cwd and build root don't go into the input root: {outside:?}",
                compilation.crate_name
            );
        }
        dep_sources.extend(inside.into_iter().filter(|p| p.is_file()));
    }
    dep_libs.extend(dep_sources);
    Ok(dep_libs)
}

fn has_extension(path: &Path, extensions: &[&str]) -> bool {
    path.extension()
        .is_some_and(|e| extensions.iter().any(|ext| e == *ext))
}

/// The variables cargo sets to paths under the cwd or build root.
const CARGO_PATH_VARS: [&str; 4] = [
    "CARGO_MANIFEST_DIR",
    "CARGO_MANIFEST_PATH",
    "OUT_DIR",
    "CARGO_TARGET_TMPDIR",
];

/// The environment of the compile's action: an allowlist of the variables cargo and rustc
/// read and the ones the crate reads. Other variables, such as `[env]` entries holding
/// local paths, would tie the action to the client machine.
fn action_env(
    compilation: &RustCompilation,
    path_mapper: &PathMapper,
    target_triple: &str,
) -> std::result::Result<ActionEnv, NotEligible> {
    let mut env = ActionEnv::default();
    // A path that doesn't map compiles locally, since dropping it would change what
    // `env!` sees on the worker.
    let map_path_var = |name: &str, value: &str| {
        path_mapper
            .to_input_root(Path::new(value))
            .map(|relative| (name.to_owned(), relative))
            .ok_or_else(|| {
                NotEligible::new(
                    CompileLocallyReason::PathOutsideInputRoot,
                    anyhow!("{name}={value}"),
                )
            })
    };
    for (name, value) in &compilation.env_vars {
        let is_env_dep = compilation
            .env_deps
            .iter()
            .any(|(dep_name, _)| dep_name == name);
        let (Some(name), Some(value)) = (name.to_str(), value.to_str()) else {
            if is_env_dep
                || name
                    .to_str()
                    .is_some_and(|name| CARGO_PATH_VARS.contains(&name))
            {
                return Err(NotEligible::new(
                    CompileLocallyReason::NonUtf8EnvVar,
                    anyhow!("{name:?}"),
                ));
            }
            continue;
        };
        match name {
            name if CARGO_PATH_VARS.contains(&name) => {
                env.path_vars.push(map_path_var(name, value)?);
            }
            "CARGO_CRATE_NAME"
            | "CARGO_PRIMARY_PACKAGE"
            | "CARGO_BIN_NAME"
            | "RUSTC_BOOTSTRAP"
            | "RUSTC_FORCE_RUSTC_VERSION" => env.vars.push((name.to_owned(), value.to_owned())),
            // rustc reads these when compiling for Apple platforms, and dep-info
            // doesn't list them.
            "MACOSX_DEPLOYMENT_TARGET"
            | "IPHONEOS_DEPLOYMENT_TARGET"
            | "TVOS_DEPLOYMENT_TARGET"
            | "WATCHOS_DEPLOYMENT_TARGET"
            | "XROS_DEPLOYMENT_TARGET"
                if target_triple.contains("-apple-") =>
            {
                env.vars.push((name.to_owned(), value.to_owned()));
            }
            name if name.starts_with("CARGO_PKG_") => {
                env.vars.push((name.to_owned(), value.to_owned()));
            }
            _ if !is_env_dep => {}
            _ if looks_like_secret(name, value) => {
                return Err(NotEligible::new(
                    CompileLocallyReason::SecretLikeEnvVar,
                    anyhow!("{name}"),
                ));
            }
            _ if Path::new(value).is_absolute() => {
                env.path_vars.push(map_path_var(name, value)?);
            }
            _ => env.vars.push((name.to_owned(), value.to_owned())),
        }
    }
    // A worker's environment may set a variable the crate reads with `option_env!`.
    env.unset_vars = compilation
        .env_deps
        .iter()
        .filter(|(dep_name, _)| {
            !compilation
                .env_vars
                .iter()
                .any(|(name, _)| name == dep_name)
        })
        .filter_map(|(dep_name, _)| dep_name.to_str().map(str::to_owned))
        .collect();
    let mut names = env
        .vars
        .iter()
        .chain(&env.path_vars)
        .map(|(name, _)| name)
        .chain(&env.unset_vars);
    if let Some(name) = names.find(|name| !reaches_rustc_unchanged(name)) {
        return Err(NotEligible::new(
            CompileLocallyReason::UnsupportedEnvVarName,
            anyhow!("{name}"),
        ));
    }
    Ok(env)
}

/// Whether a variable the crate reads looks like it holds a secret, which the action's
/// command would store on the REAPI server: by its name, or by a URL with credentials.
fn looks_like_secret(name: &str, value: &str) -> bool {
    let name = name.to_ascii_uppercase();
    let secret_name = [
        "TOKEN",
        "SECRET",
        "PASSWORD",
        "PASSWD",
        "CREDENTIAL",
        "CREDS",
        "PRIVATE",
        "KEY",
        "AUTH",
        "DSN",
    ]
    .iter()
    .any(|word| name.contains(word));
    let url_with_credentials = value
        .split_once("://")
        .and_then(|(_, rest)| rest.split(['/', '?', '#']).next())
        .is_some_and(|authority| authority.contains('@'));
    secret_name || url_with_credentials
}

/// Whether a variable named `name` reaches rustc as it is. The REAPI command's shell
/// script sets `PWD`, `OLDPWD` and the library paths, a shell may drop names that aren't
/// identifiers, and a worker's `/bin/sh` may be bash, which sets `SHLVL` and `_` and
/// can't unset its read-only variables.
fn reaches_rustc_unchanged(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !matches!(
            name,
            "PWD"
                | "OLDPWD"
                | "LD_LIBRARY_PATH"
                | "DYLD_LIBRARY_PATH"
                | "IFS"
                | "SHLVL"
                | "_"
                | "UID"
                | "EUID"
                | "PPID"
                | "SHELLOPTS"
                | "BASHOPTS"
                | "BASH_VERSINFO"
        )
}

#[cfg(all(test, unix))]
mod test {
    use super::*;
    use crate::test::utils::create_file;
    use std::os::unix::ffi::OsStringExt;
    use test_case::test_case;

    const CWD: &str = "/ws/app";
    const HOST: &str = "x86_64-unknown-linux-gnu";

    /// Writes its outputs to `target_dir`'s `debug/deps`, which is also its -L dependency dir.
    fn compilation_with_target_dir(
        cwd: &str,
        target_dir: &str,
        crate_root: &str,
        extra: &[&str],
    ) -> RustCompilation {
        let deps = format!("{target_dir}/debug/deps");
        let dependency = format!("dependency={deps}");
        let mut args = vec![
            "--crate-name",
            "foo",
            "--edition=2021",
            crate_root,
            "--crate-type",
            "lib",
            "--emit=dep-info,metadata,link",
            "-C",
            "metadata=abc",
            "--out-dir",
            &deps,
            "-L",
            &dependency,
        ];
        args.extend(extra);
        let args: Vec<OsString> = args.into_iter().map(OsString::from).collect();
        let parsed = match parse_arguments(&args, Path::new(cwd), &[]) {
            CompilerArguments::Ok(parsed) => parsed,
            o => panic!("unexpected parse result: {o:?}"),
        };
        RustCompilation {
            executable: "/toolchain/bin/rustc".into(),
            env_deps: vec![],
            host: HOST.to_owned(),
            sysroot: "/toolchain".into(),
            commit_hash: Some("abc123".to_owned()),
            #[cfg(feature = "dist-client")]
            target_libdir: format!("/toolchain/lib/rustlib/{HOST}/lib").into(),
            #[cfg(any(feature = "dist-client", feature = "reapi"))]
            rlib_dep_reader: None,
            arguments: parsed.arguments,
            inputs: vec![crate_root.into()],
            outputs: [(
                "link".to_owned(),
                ArtifactDescriptor {
                    path: format!("{deps}/libfoo-abc.rlib").into(),
                    optional: false,
                },
            )]
            .into(),
            crate_link_paths: parsed.crate_link_paths,
            crate_name: "foo".to_owned(),
            crate_types: parsed.crate_types,
            cwd: cwd.into(),
            env_vars: vec![],
        }
    }

    fn compilation_in(cwd: &str, crate_root: &str, extra: &[&str]) -> RustCompilation {
        compilation_with_target_dir(cwd, &format!("{cwd}/target"), crate_root, extra)
    }

    fn compilation(extra: &[&str]) -> RustCompilation {
        compilation_in(CWD, "src/lib.rs", extra)
    }

    /// `compilation` with the environment `vars`, of which the crate reads `env_deps`.
    fn with_env(
        mut compilation: RustCompilation,
        vars: Vec<(OsString, OsString)>,
        env_deps: &[&str],
    ) -> RustCompilation {
        compilation.env_deps = env_deps
            .iter()
            .map(|dep| {
                let value = vars.iter().find(|(name, _)| name == dep);
                (dep.into(), value.map(|(_, v)| v.clone()))
            })
            .collect();
        compilation.env_vars = vars;
        compilation
    }

    fn env(vars: &[(&str, &str)]) -> Vec<(OsString, OsString)> {
        vars.iter().map(|(k, v)| (k.into(), v.into())).collect()
    }

    /// A reader whose cache answers `rustc -Z ls` for each of `rlibs` with its stems.
    fn rlib_dep_reader(rlibs: &[(&Path, Vec<String>)]) -> Arc<RlibDepReader> {
        let reader = RlibDepReader {
            cache: Mutex::new(LruCache::with_meter(1 << 20, DepsSize)),
            executable: "/toolchain/bin/rustc".into(),
            ls_arg: "ls=root".to_owned(),
        };
        for (rlib, stems) in rlibs {
            reader.cache.lock().unwrap().insert(
                rlib.to_path_buf(),
                RlibDepsDetail {
                    stems: stems.clone(),
                    mtime: fs::metadata(rlib).unwrap().modified().unwrap(),
                },
            );
        }
        Arc::new(reader)
    }

    fn compilation_with_externs(root: &Path, externs: &[&Path]) -> RustCompilation {
        let mut extra = vec![];
        for (i, extern_lib) in externs.iter().enumerate() {
            extra.push("--extern".to_owned());
            extra.push(format!("dep{i}={}", extern_lib.display()));
        }
        let extra: Vec<&str> = extra.iter().map(String::as_str).collect();
        compilation_in(root.to_str().unwrap(), "src/lib.rs", &extra)
    }

    /// A target dir in a new temp dir whose `debug/deps` holds `files`, all empty.
    fn deps_dir_with(files: &[&str]) -> (tempfile::TempDir, PathBuf, PathBuf) {
        let tempdir = tempfile::tempdir().unwrap();
        // Canonical, like the paths create_file returns.
        let root = tempdir.path().canonicalize().unwrap();
        let deps = root.join("target/debug/deps");
        for file in files {
            create_file(&deps, file, |_| Ok(())).unwrap();
        }
        (tempdir, root, deps)
    }

    fn owned(vars: &[(&str, &str)]) -> Vec<(String, String)> {
        vars.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn with(
        mut compilation: RustCompilation,
        change: impl FnOnce(&mut RustCompilation),
    ) -> RustCompilation {
        change(&mut compilation);
        compilation
    }

    #[test]
    fn test_deps_dir_base() {
        let base = |d: &str| deps_dir_base(Path::new(d)).map(|p| p.to_str().unwrap().to_owned());
        assert_eq!(Some("/w/target"), base("/w/target/debug/deps").as_deref());
        assert_eq!(
            Some("/w/target/aarch64-apple-darwin"),
            base("/w/target/aarch64-apple-darwin/release/deps").as_deref()
        );
        assert_eq!(None, base("/w/target/debug/examples"));
        assert_eq!(
            None,
            base("/w/target/debug/build/memchr-1a6535e85f000ff2/out")
        );
    }

    #[test]
    fn test_maps_paths_into_the_input_root() {
        let compile = map_for_reapi(&compilation(&[])).unwrap();
        assert!(
            compile
                .arguments
                .iter()
                .any(|a| a == "dependency=.sccache-build/debug/deps"),
            "{:?}",
            compile.arguments
        );
        assert!(
            !compile.arguments.iter().any(|a| a.contains("/ws/app")),
            "{:?}",
            compile.arguments
        );
        assert_eq!(
            vec![PathBuf::from(format!("{CWD}/src/lib.rs"))],
            compile.input_root_files
        );
        assert_eq!(
            vec![PathBuf::from(format!(
                "{CWD}/target/debug/deps/libfoo-abc.rlib"
            ))],
            compile.outputs
        );
    }

    #[test_case(
        compilation_in("/a/app", "src/lib.rs", &[]),
        compilation_in("/b/app", "src/lib.rs", &[]) ;
        "target dir in the cwd"
    )]
    #[test_case(
        compilation_with_target_dir("/a/ws/member", "/a/ws/target", "src/lib.rs", &[]),
        compilation_with_target_dir("/b/ws/member", "/b/ws/target", "src/lib.rs", &[]) ;
        "workspace member with the target dir in the workspace root"
    )]
    fn test_two_checkouts_give_the_same_arguments(a: RustCompilation, b: RustCompilation) {
        let (a, b) = (map_for_reapi(&a).unwrap(), map_for_reapi(&b).unwrap());
        assert_eq!(a.arguments, b.arguments);
        let mapped_inputs = |compile: &ReapiCompile| -> Vec<String> {
            compile
                .input_root_files
                .iter()
                .map(|p| compile.path_mapper.to_input_root(p).unwrap())
                .collect()
        };
        assert_eq!(mapped_inputs(&a), mapped_inputs(&b));
    }

    #[test]
    fn test_the_diagnostic_width_isnt_sent() {
        let without = map_for_reapi(&compilation(&[])).unwrap();
        for args in [
            &["--diagnostic-width=120"][..],
            &["--diagnostic-width", "211"],
        ] {
            let with = map_for_reapi(&compilation(args)).unwrap();
            assert_eq!(without.arguments, with.arguments);
        }
    }

    #[test_case("/ws/target/debug/deps" ; "absolute")]
    #[test_case("../target/debug/deps" ; "relative")]
    fn test_a_target_dir_outside_the_cwd_is_the_build_root(deps: &str) {
        let mut compilation = compilation_with_target_dir(
            "/ws/member",
            "/ws/target",
            "src/lib.rs",
            &["-L", &format!("dependency={deps}")],
        );
        compilation.outputs.get_mut("link").unwrap().path =
            format!("{deps}/libfoo-abc.rlib").into();
        let compile = map_for_reapi(&compilation).unwrap();
        assert_eq!(
            Some(".sccache-build/debug/deps".to_owned()),
            compile
                .path_mapper
                .to_input_root(Path::new("/ws/target/debug/deps"))
        );
        assert_eq!(
            Some("src/lib.rs".to_owned()),
            compile
                .path_mapper
                .to_input_root(Path::new("/ws/member/src/lib.rs"))
        );
    }

    #[test]
    fn test_passes_cargo_and_rustc_vars_and_the_vars_the_crate_reads() {
        let compilation = with_env(
            compilation(&[]),
            env(&[
                ("CARGO_MANIFEST_DIR", CWD),
                ("OUT_DIR", "/ws/app/target/debug/build/foo-1/out"),
                ("CARGO_PKG_NAME", "foo"),
                ("CARGO_PKG_AUTHORS", "Ferris"),
                ("CARGO_CRATE_NAME", "foo"),
                ("CARGO_PRIMARY_PACKAGE", "1"),
                ("RUSTC_BOOTSTRAP", "1"),
                ("HOME", "/home/me"),
                ("FOO_LEVEL", "3"),
                ("FOO_DATA", "/ws/app/data"),
                ("UNREAD", "x"),
                ("GITHUB_TOKEN", "ghp_x"),
            ]),
            &["FOO_LEVEL", "FOO_DATA", "CARGO_PKG_AUTHORS"],
        );
        let env = map_for_reapi(&compilation).unwrap().env;
        assert_eq!(
            owned(&[
                ("CARGO_MANIFEST_DIR", "."),
                ("OUT_DIR", ".sccache-build/debug/build/foo-1/out"),
                ("FOO_DATA", "data"),
            ]),
            env.path_vars
        );
        assert_eq!(
            owned(&[
                ("CARGO_PKG_NAME", "foo"),
                ("CARGO_PKG_AUTHORS", "Ferris"),
                ("CARGO_CRATE_NAME", "foo"),
                ("CARGO_PRIMARY_PACKAGE", "1"),
                ("RUSTC_BOOTSTRAP", "1"),
                ("FOO_LEVEL", "3"),
            ]),
            env.vars
        );
    }

    #[test]
    fn test_vars_the_crate_reads_that_are_unset_stay_unset() {
        let compilation = with_env(compilation(&[]), env(&[("CI", "1")]), &["HOSTNAME"]);
        let env = map_for_reapi(&compilation).unwrap().env;
        assert_eq!(vec!["HOSTNAME".to_owned()], env.unset_vars);
        assert!(env.vars.is_empty(), "{env:?}");
    }

    #[test_case("x86_64-unknown-linux-gnu", false ; "linux")]
    #[test_case("aarch64-apple-darwin", true ; "macos")]
    fn test_apple_deployment_targets_are_passed_for_apple_targets(target: &str, passed: bool) {
        let compilation = with_env(
            compilation(&["--target", target]),
            env(&[("MACOSX_DEPLOYMENT_TARGET", "11.0")]),
            &[],
        );
        let env = map_for_reapi(&compilation).unwrap().env;
        assert_eq!(passed, !env.vars.is_empty(), "{env:?}");
    }

    #[test]
    fn test_the_crates_in_the_externs_graphs_go_into_the_input_root() {
        let proc_macro = format!("libpm-2.{DLL_EXTENSION}");
        let (_tempdir, root, deps) = deps_dir_with(&[
            "libfoo-0.rmeta",
            "foo-0.d",
            "libbar-1.rmeta",
            "libbar-1.rlib",
            "bar-1.d",
            "libbaz-3.rlib",
            "baz-3.d",
            &proc_macro,
            "pm-2.d",
            "liblibc-other.rmeta",
        ]);
        let foo = deps.join("libfoo-0.rmeta");
        let mut compilation = compilation_with_externs(&root, &[&foo]);
        compilation.rlib_dep_reader = Some(rlib_dep_reader(&[(
            &foo,
            vec![
                "bar-1".into(),
                "baz-3".into(),
                "pm-2".into(),
                "libc-e47c".into(),
            ],
        )]));
        let files = map_for_reapi(&compilation).unwrap().input_root_files;
        for file in [
            deps.join("libbar-1.rmeta"),
            deps.join("libbaz-3.rlib"),
            deps.join(&proc_macro),
        ] {
            assert!(files.contains(&file), "{file:?} not in {files:?}");
        }
        for file in [deps.join("libbar-1.rlib"), deps.join("liblibc-other.rmeta")] {
            assert!(!files.contains(&file), "{file:?} in {files:?}");
        }
    }

    #[test]
    fn test_dependency_sources_inside_the_roots_go_into_the_input_root() {
        let (_tempdir, root, deps) = deps_dir_with(&["libfoo-0.rmeta"]);
        let outside_dir = tempfile::tempdir().unwrap();
        let outside_source = create_file(outside_dir.path(), "x.rs", |_| Ok(())).unwrap();
        fs::write(
            deps.join("foo-0.d"),
            format!(
                "{}: src/foo.rs {}\n",
                deps.join("foo-0.d").display(),
                outside_source.display()
            ),
        )
        .unwrap();
        create_file(&root, "src/foo.rs", |_| Ok(())).unwrap();
        // A file the crate reads with `include_bytes!`, named like a lib but without dep-info.
        let lib_named_source =
            create_file(&root, &format!("src/libdata.{DLL_EXTENSION}"), |_| Ok(())).unwrap();
        let foo = deps.join("libfoo-0.rmeta");
        let mut compilation = compilation_with_externs(&root, &[&foo]);
        compilation.inputs.push(lib_named_source.clone());
        compilation.rlib_dep_reader = Some(rlib_dep_reader(&[(&foo, vec![])]));
        let files = map_for_reapi(&compilation).unwrap().input_root_files;
        for file in [root.join("src/foo.rs"), lib_named_source] {
            assert!(files.contains(&file), "{file:?} not in {files:?}");
        }
        assert!(!files.contains(&outside_source), "{files:?}");
    }

    #[test]
    fn test_a_dependency_without_dep_info_compiles_locally() {
        let (_tempdir, root, deps) = deps_dir_with(&["libfoo-0.rmeta"]);
        let foo = deps.join("libfoo-0.rmeta");
        let mut compilation = compilation_with_externs(&root, &[&foo]);
        compilation.rlib_dep_reader = Some(rlib_dep_reader(&[(&foo, vec![])]));
        assert_eq!(
            CompileLocallyReason::UnreadableDependencies,
            map_for_reapi(&compilation).unwrap_err().reason
        );
    }

    #[test]
    fn test_externs_without_an_rlib_dependency_reader_compile_locally() {
        let (_tempdir, root, deps) = deps_dir_with(&["libfoo-0.rmeta", "foo-0.d"]);
        let compilation = compilation_with_externs(&root, &[&deps.join("libfoo-0.rmeta")]);
        assert_eq!(
            CompileLocallyReason::UnreadableDependencies,
            map_for_reapi(&compilation).unwrap_err().reason
        );
    }

    #[test]
    fn test_a_crate_without_externs_needs_no_rlib_dependency_reader() {
        let (_tempdir, root, _deps) = deps_dir_with(&[]);
        map_for_reapi(&compilation_with_externs(&root, &[])).unwrap();
    }

    #[test]
    fn test_input_root_files_are_sorted_and_deduplicated() {
        let (_tempdir, root, deps) =
            deps_dir_with(&["libbar-1.rmeta", "libfoo-0.rmeta", "bar-1.d", "foo-0.d"]);
        let (bar, foo) = (deps.join("libbar-1.rmeta"), deps.join("libfoo-0.rmeta"));
        let mut compilation = compilation_with_externs(&root, &[&foo, &bar]);
        compilation.rlib_dep_reader = Some(rlib_dep_reader(&[(&foo, vec![]), (&bar, vec![])]));
        compilation.inputs.extend([bar.clone(), foo.clone()]);
        assert_eq!(
            vec![root.join("src/lib.rs"), bar, foo],
            map_for_reapi(&compilation).unwrap().input_root_files
        );
    }

    #[test]
    fn test_the_directories_an_input_steps_out_of_go_into_the_input_root() {
        let mut compilation = compilation(&[]);
        compilation
            .inputs
            .push(format!("{CWD}/src/sys/bits/../../x86/mutex.rs").into());
        assert_eq!(
            vec!["src/sys/bits", "src/sys"],
            map_for_reapi(&compilation).unwrap().input_root_directories
        );
    }

    #[test]
    fn test_passes_the_target_triple() {
        let compile = map_for_reapi(&compilation(&[])).unwrap();
        assert_eq!(HOST, compile.toolchain.as_rustc().target_triple);
        assert!(compile.arguments.contains(&format!("--target={HOST}")));

        let compile =
            map_for_reapi(&compilation(&["--target", "aarch64-unknown-linux-gnu"])).unwrap();
        assert_eq!(
            "aarch64-unknown-linux-gnu",
            compile.toolchain.as_rustc().target_triple
        );
        assert!(!compile.arguments.contains(&format!("--target={HOST}")));
        assert!(
            compile
                .arguments
                .windows(2)
                .any(|w| w == ["--target", "aarch64-unknown-linux-gnu"]),
            "{:?}",
            compile.arguments
        );
    }

    #[test]
    fn test_an_absolute_crate_root_is_left_out_of_the_arguments() {
        let compilation = compilation_in(CWD, "/ws/app/src/lib.rs", &[]);
        let compile = map_for_reapi(&compilation).unwrap();
        assert!(
            matches!(
                &compile.program,
                WorkerProgram::Rustc { mapped_crate_root: Some(root) } if root == "src/lib.rs"
            ),
            "{:?}",
            compile.program
        );
        assert!(
            !compile.arguments.iter().any(|a| a.contains("lib.rs")),
            "{:?}",
            compile.arguments
        );
    }

    #[test]
    fn test_build_root_holds_the_target_and_host_deps_dirs() {
        let target_deps = "/ws/app/target/aarch64-unknown-linux-gnu/debug/deps";
        let mut compilation = compilation(&[
            "--target",
            "aarch64-unknown-linux-gnu",
            "-L",
            &format!("dependency={target_deps}"),
        ]);
        compilation.outputs.get_mut("link").unwrap().path =
            format!("{target_deps}/libfoo-abc.rlib").into();
        let compile = map_for_reapi(&compilation).unwrap();
        assert_eq!(
            Some(".sccache-build/aarch64-unknown-linux-gnu/debug/deps".to_owned()),
            compile.path_mapper.to_input_root(Path::new(target_deps))
        );
        assert_eq!(
            Some(".sccache-build/debug/deps".to_owned()),
            compile
                .path_mapper
                .to_input_root(Path::new("/ws/app/target/debug/deps"))
        );
    }

    #[test]
    fn test_a_link_dir_outside_deps_is_mapped_as_an_argument() {
        let out_dir = "/ws/app/target/debug/build/foo-123/out";
        let compile = map_for_reapi(&compilation(&["-L", out_dir])).unwrap();
        assert_eq!(
            Some(".sccache-build".to_owned()),
            compile
                .path_mapper
                .to_input_root(Path::new("/ws/app/target"))
        );
        assert!(
            compile
                .arguments
                .contains(&"all=.sccache-build/debug/build/foo-123/out".to_owned()),
            "{:?}",
            compile.arguments
        );
    }

    #[test_case("rustc 1.91.0\ncommit-hash: abc123\nhost: x\n", Some("abc123") ; "release")]
    #[test_case("rustc 1.91.0-dev\ncommit-hash: unknown\n", None ; "built without git")]
    #[test_case("rustc 1.91.0\n", None ; "no commit-hash line")]
    fn test_parse_commit_hash(version: &str, expected: Option<&str>) {
        assert_eq!(expected.map(str::to_owned), parse_commit_hash(version));
    }

    #[test_case(compilation(&["-C", "target-cpu=x86-64-v3"]) ; "named target cpu")]
    #[test_case(compilation(&["-C", "linker=/usr/bin/clang"]) ; "absolute linker")]
    #[test_case(compilation(&["-C", "link-arg=/opt/lib/crt0.o"]) ; "absolute link arg")]
    fn test_eligible_compiles(compilation: RustCompilation) {
        map_for_reapi(&compilation).unwrap();
    }

    #[test_case(
        CompileLocallyReason::RustcDriver,
        with(compilation(&[]), |c| c.executable = "/toolchain/bin/clippy-driver".into()) ;
        "clippy driver"
    )]
    #[test_case(
        CompileLocallyReason::NoCommitHash,
        with(compilation(&[]), |c| c.commit_hash = None) ;
        "no commit hash"
    )]
    #[test_case(
        CompileLocallyReason::Staticlib,
        with(compilation(&[]), |c| c.crate_types.staticlib = true) ;
        "staticlib"
    )]
    #[test_case(
        CompileLocallyReason::CustomTargetSpec,
        compilation(&["--target", "/ws/app/foo.json"]) ;
        "target spec file"
    )]
    #[test_case(
        CompileLocallyReason::NativeCpu,
        compilation(&["-C", "target-cpu=native"]) ;
        "native target cpu"
    )]
    #[test_case(
        CompileLocallyReason::NativeCpu,
        compilation(&["-Z", "tune-cpu=native"]) ;
        "native tune cpu"
    )]
    #[test_case(
        CompileLocallyReason::SourceOutsideInputRoot,
        with(compilation(&[]), |c| c.inputs.push("/elsewhere/data.json".into())) ;
        "source outside"
    )]
    #[test_case(
        CompileLocallyReason::PathOutsideInputRoot,
        compilation_in(CWD, "../other/src/lib.rs", &[]) ;
        "crate root outside"
    )]
    #[test_case(
        CompileLocallyReason::PathOutsideInputRoot,
        compilation(&["-L", "/opt/lib"]) ;
        "link dir outside"
    )]
    #[test_case(
        CompileLocallyReason::ProfileUse,
        compilation(&["-C", "profile-use=/ws/app/pgo.profdata"]) ;
        "profile use"
    )]
    #[test_case(
        CompileLocallyReason::ProfileUse,
        compilation(&["-C", "profile-sample-use=/ws/app/perf.prof"]) ;
        "profile sample use"
    )]
    #[test_case(
        CompileLocallyReason::ProfileUse,
        compilation(&["-Z", "profile-sample-use=/ws/app/perf.prof"]) ;
        "unstable profile sample use"
    )]
    #[test_case(
        CompileLocallyReason::AbsolutePathInOption,
        compilation(&["-Z", "codegen-backend=/opt/cg.so"]) ;
        "absolute path in a -Z option"
    )]
    #[test_case(
        CompileLocallyReason::RemapPathPrefix,
        compilation(&["--remap-path-prefix", "/ws/app=/build"]) ;
        "remap path prefix"
    )]
    #[test_case(
        CompileLocallyReason::UnexpectedCargoLayout,
        with(compilation(&[]), |c| c.outputs.clear()) ;
        "no outputs"
    )]
    #[test_case(
        CompileLocallyReason::UnexpectedCargoLayout,
        with(compilation(&[]), |c| {
            c.outputs.get_mut("link").unwrap().path =
                "/ws/app/target/debug/examples/libfoo-abc.rlib".into();
        }) ;
        "output dir not deps"
    )]
    #[test_case(
        CompileLocallyReason::BuildScriptRustc,
        with_env(
            with(compilation(&[]), |c| {
                c.outputs.get_mut("link").unwrap().path =
                    "/ws/app/target/debug/build/foo-1/out/probe/libfoo.rmeta".into();
            }),
            env(&[("OUT_DIR", "/ws/app/target/debug/build/foo-1/out")]),
            &[],
        ) ;
        "output in OUT_DIR"
    )]
    #[test_case(
        CompileLocallyReason::BuildScriptRustc,
        with_env(
            with(compilation(&[]), |c| {
                c.outputs.get_mut("link").unwrap().path = "target/debug/build/foo-1/out/libfoo.rmeta".into();
            }),
            env(&[("OUT_DIR", "../app/target/debug/build/foo-1/out")]),
            &[],
        ) ;
        "output in a relative OUT_DIR"
    )]
    #[test_case(
        CompileLocallyReason::UnexpectedCargoLayout,
        with_env(
            with(compilation(&[]), |c| {
                c.outputs.get_mut("link").unwrap().path =
                    "/ws/app/target/debug/examples/libfoo-abc.rlib".into();
            }),
            env(&[
                ("CARGO_MANIFEST_DIR", "/ws/app"),
                ("OUT_DIR", "/ws/app/target/debug/build/foo-1/out"),
            ]),
            &[],
        ) ;
        "output dir neither deps nor OUT_DIR"
    )]
    #[test_case(
        CompileLocallyReason::UnexpectedCargoLayout,
        compilation(&["-L", "dependency=/ws/other/target/debug/deps"]) ;
        "deps dirs in two target dirs"
    )]
    #[test_case(
        CompileLocallyReason::UnexpectedCargoLayout,
        compilation(&["-L", "dependency=/other/debug/deps"]) ;
        "build root holds the cwd"
    )]
    #[test_case(
        CompileLocallyReason::PathOutsideInputRoot,
        with_env(compilation(&[]), env(&[("CARGO_MANIFEST_DIR", "/elsewhere")]), &[]) ;
        "cargo path var outside"
    )]
    #[test_case(
        CompileLocallyReason::PathOutsideInputRoot,
        with_env(compilation(&[]), env(&[("FOO_DIR", "/elsewhere")]), &["FOO_DIR"]) ;
        "path var the crate reads outside"
    )]
    #[test_case(
        CompileLocallyReason::NonUtf8EnvVar,
        with_env(
            compilation(&[]),
            vec![("FOO_LEVEL".into(), OsString::from_vec(vec![0xff]))],
            &["FOO_LEVEL"],
        ) ;
        "non-UTF-8 var the crate reads"
    )]
    #[test_case(
        CompileLocallyReason::NonUtf8EnvVar,
        with_env(
            compilation(&[]),
            vec![("OUT_DIR".into(), OsString::from_vec(vec![0xff]))],
            &[],
        ) ;
        "non-UTF-8 cargo path var"
    )]
    #[test_case(
        CompileLocallyReason::SecretLikeEnvVar,
        with_env(compilation(&[]), env(&[("SENTRY_API_TOKEN", "x")]), &["SENTRY_API_TOKEN"]) ;
        "token the crate reads"
    )]
    #[test_case(
        CompileLocallyReason::SecretLikeEnvVar,
        with_env(compilation(&[]), env(&[("AWS_ACCESS_KEY_ID", "x")]), &["AWS_ACCESS_KEY_ID"]) ;
        "key the crate reads"
    )]
    #[test_case(
        CompileLocallyReason::SecretLikeEnvVar,
        with_env(compilation(&[]), env(&[("SENTRY_DSN", "x")]), &["SENTRY_DSN"]) ;
        "dsn the crate reads"
    )]
    #[test_case(
        CompileLocallyReason::SecretLikeEnvVar,
        with_env(
            compilation(&[]),
            env(&[("DATABASE_URL", "postgres://app:hunter2@db:5432/app")]),
            &["DATABASE_URL"],
        ) ;
        "url with credentials the crate reads"
    )]
    #[test_case(
        CompileLocallyReason::UnsupportedEnvVarName,
        with_env(compilation(&[]), env(&[("FOO-LEVEL", "1")]), &["FOO-LEVEL"]) ;
        "var name a shell drops"
    )]
    #[test_case(
        CompileLocallyReason::UnsupportedEnvVarName,
        with_env(compilation(&[]), env(&[("PWD", "/ws/app")]), &["PWD"]) ;
        "var the command sets"
    )]
    #[test_case(
        CompileLocallyReason::UnsupportedEnvVarName,
        with_env(compilation(&[]), env(&[]), &["FOO-LEVEL"]) ;
        "unset var name a shell can't unset"
    )]
    #[test_case(
        CompileLocallyReason::UnsupportedEnvVarName,
        with_env(compilation(&[]), env(&[]), &["UID"]) ;
        "unset var bash can't unset"
    )]
    #[test_case(
        CompileLocallyReason::CustomTargetSpec,
        with_env(compilation(&[]), env(&[("RUST_TARGET_PATH", "/specs")]), &[]) ;
        "target spec path"
    )]
    fn test_not_eligible_compiles(reason: CompileLocallyReason, compilation: RustCompilation) {
        match map_for_reapi(&compilation) {
            Ok(compile) => panic!("eligible: {compile:?}"),
            Err(e) => assert_eq!(reason, e.reason),
        }
    }

    /// `compilation(extra)` with a sysroot in `dir`.
    fn compilation_with_sysroot(dir: &Path, extra: &[&str]) -> RustCompilation {
        let mut c = compilation(extra);
        c.sysroot = dir.join("toolchain");
        std::fs::create_dir_all(c.sysroot.join("lib/rustlib")).unwrap();
        c
    }

    /// The arguments `changes` appends and their hash key forms.
    fn args_and_hashed(changes: &LocalCompileChanges) -> (Vec<String>, Vec<String>) {
        changes
            .args
            .iter()
            .map(|arg| {
                (
                    arg.arg.to_str().unwrap().to_owned(),
                    arg.hashed_as.to_str().unwrap().to_owned(),
                )
            })
            .unzip()
    }

    #[test]
    fn test_local_compile_changes_remap_paths_and_use_a_sysroot_copy() {
        let dir = tempfile::tempdir().unwrap();
        let sysroot_copies_dir = dir.path().join("sysroot-copies");
        let c = compilation_with_sysroot(dir.path(), &[]);
        let changes = reapi_local_compile_changes(&c, &sysroot_copies_dir).unwrap();
        let copy = crate::reapi::sysroot_copy(&c.sysroot, &sysroot_copies_dir).unwrap();
        assert_eq!(
            [
                "--remap-path-prefix=/ws/app=.".to_owned(),
                "--remap-path-prefix=/ws/app/target=.sccache-build".to_owned(),
                format!("--sysroot={}", copy.display())
            ],
            args_and_hashed(&changes).0.as_slice()
        );
    }

    #[test]
    fn test_local_compile_changes_leave_the_sysroot_copy_out_of_the_hash_key() {
        let dir = tempfile::tempdir().unwrap();
        let c = compilation_with_sysroot(dir.path(), &[]);
        let changes = reapi_local_compile_changes(&c, &dir.path().join("sysroot-copies")).unwrap();
        assert_eq!(
            [
                "--remap-path-prefix=/ws/app=.",
                "--remap-path-prefix=/ws/app/target=.sccache-build",
                "--sysroot=<sysroot copy>"
            ],
            args_and_hashed(&changes).1.as_slice()
        );
    }

    #[test]
    fn test_local_compile_changes_map_build_root_paths_in_stderr() {
        let dir = tempfile::tempdir().unwrap();
        let c = compilation_with_sysroot(dir.path(), &[]);
        let changes = reapi_local_compile_changes(&c, &dir.path().join("sysroot-copies")).unwrap();
        let map = changes.stdout_stderr_mapper.unwrap();
        assert_eq!(
            b"--> /ws/app/target/debug/build/foo-1/out/gen.rs:3:5".as_slice(),
            map(b"--> .sccache-build/debug/build/foo-1/out/gen.rs:3:5")
        );
    }

    #[test]
    fn test_no_local_compile_changes_for_a_compile_that_remaps_paths() {
        let dir = tempfile::tempdir().unwrap();
        let c = compilation_with_sysroot(dir.path(), &["--remap-path-prefix=/ws/app=/build"]);
        assert!(reapi_local_compile_changes(&c, &dir.path().join("sysroot-copies")).is_none());
    }

    #[test]
    fn test_no_local_compile_changes_for_a_rustc_driver() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = compilation_with_sysroot(dir.path(), &[]);
        c.executable = "/toolchain/bin/clippy-driver".into();
        assert!(reapi_local_compile_changes(&c, &dir.path().join("sysroot-copies")).is_none());
    }

    #[test]
    fn test_no_local_compile_changes_without_a_sysroot_copy() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = compilation(&[]);
        c.sysroot = dir.path().join("missing");
        assert!(reapi_local_compile_changes(&c, &dir.path().join("sysroot-copies")).is_none());
    }
}
