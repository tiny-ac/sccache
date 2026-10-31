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

//! gcc and clang compiles run by a cargo build script, mapped for REAPI. A worker compiles the
//! source preprocessed on the client, so it needs no headers, and the action's arguments are
//! sccache-dist's, less the options that a compile of a preprocessed source doesn't read: no
//! `-I`, `-D`, `-M`, `--sysroot` or link options.

use crate::compiler::Language;
use crate::compiler::c::{CCompilerKind, ParsedArguments};
use crate::reapi::{
    CToolchainKey, CompileLocallyReason, InternalFile, NotEligible, PathMapper, PreprocessedSource,
    ReapiCompile, ToolchainKey, WorkerProgram, lexically_normalize,
};
use anyhow::anyhow;
use std::ffi::OsString;
use std::io::Write;
use std::path::{Path, PathBuf};
use tempfile::TempPath;

/// Prefixes of the options a worker's compile leaves out: it reads a preprocessed source and
/// doesn't link.
const LEFT_OUT_PREFIXES: &[&str] = &["-D", "-U", "--sysroot=", "-L", "-Wl,"];

/// Env vars that change which programs the compiler runs or what it does, which a worker
/// doesn't get.
const COMPILER_ENV_VARS: &[&str] = &[
    "GCC_EXEC_PREFIX",
    "COMPILER_PATH",
    "CCC_OVERRIDE_OPTIONS",
    "GCC_COMPARE_DEBUG",
];

/// Prefixes of the options that write files besides the object and the depfile.
const SIDE_OUTPUT_PREFIXES: &[&str] = &[
    "-fdump-",
    "-save-temps",
    "--save-temps",
    "-ftime-trace",
    "-fcallgraph-info",
    "-fdiagnostics-add-output",
    "-fdiagnostics-set-output",
    "-fsave-optimization-record",
    "-foptimization-record-file",
];

/// Options that print the path of a file gcc runs or reads to compile: its compilers proper,
/// its assembler, and its specs file if it has one.
const GCC_INTERNAL_FILES: &[&str] = &[
    "-print-prog-name=cc1",
    "-print-prog-name=cc1plus",
    "-print-prog-name=as",
    "-print-file-name=specs",
];

/// Off while workers can't run a C toolchain, which keeps every C compile local.
const WORKERS_RUN_C_COMPILES: bool = false;

/// The compile of `parsed_args` by `executable` in `cwd`, mapped for REAPI with its
/// preprocessed source under `preprocessed_dir`, or why it runs locally. Runs the compiler, so
/// call it from a blocking thread.
pub(super) fn map_for_reapi(
    kind: &CCompilerKind,
    parsed_args: &ParsedArguments,
    executable: &Path,
    cwd: &Path,
    env_vars: &[(OsString, OsString)],
    preprocessed_dir: &Path,
) -> Result<ReapiCompile, NotEligible> {
    let mapped = map_compile(kind, parsed_args, cwd, env_vars)?;
    if !WORKERS_RUN_C_COMPILES {
        return Err(CompileLocallyReason::CToolchainUnsupported.into());
    }
    reapi_compile(
        mapped,
        kind,
        parsed_args,
        executable,
        cwd,
        env_vars,
        preprocessed_dir,
    )
}

/// The `ReapiCompile` of `mapped`, with its toolchain and preprocessed source.
fn reapi_compile(
    mapped: MappedCompile,
    kind: &CCompilerKind,
    parsed_args: &ParsedArguments,
    executable: &Path,
    cwd: &Path,
    env_vars: &[(OsString, OsString)],
    preprocessed_dir: &Path,
) -> Result<ReapiCompile, NotEligible> {
    let MappedCompile {
        path_mapper,
        arguments,
        mapped_input,
        object,
    } = mapped;
    let toolchain = toolchain_key(kind, executable, &parsed_args.language, cwd, env_vars)?;
    if *kind == CCompilerKind::Clang {
        check_clang_config(executable, &toolchain.driver, cwd, env_vars)?;
    }
    let preprocessed = preprocess_for_reapi(
        kind,
        parsed_args,
        executable,
        cwd,
        env_vars,
        &path_mapper,
        preprocessed_dir,
    )?;
    Ok(ReapiCompile {
        path_mapper,
        arguments,
        program: WorkerProgram::C,
        toolchain: ToolchainKey::C(toolchain),
        env: Default::default(),
        input_root_files: vec![],
        preprocessed_source: Some(PreprocessedSource::new(preprocessed, mapped_input)),
        outputs: vec![object],
    })
}

/// A compile's arguments and paths mapped for REAPI.
struct MappedCompile {
    path_mapper: PathMapper,
    arguments: Vec<String>,
    /// The source's path in the input root, where a worker reads the preprocessed source.
    mapped_input: String,
    object: PathBuf,
}

/// The arguments and paths of the compile of `parsed_args` in `cwd`, mapped for REAPI, or why
/// it runs locally.
fn map_compile(
    kind: &CCompilerKind,
    parsed_args: &ParsedArguments,
    cwd: &Path,
    env_vars: &[(OsString, OsString)],
) -> Result<MappedCompile, NotEligible> {
    // PathMapper doesn't convert Windows paths to the /-separated paths REAPI uses.
    if cfg!(windows) {
        return Err(CompileLocallyReason::WindowsPaths.into());
    }
    if !matches!(kind, CCompilerKind::Gcc | CCompilerKind::Clang) {
        return Err(NotEligible::new(
            CompileLocallyReason::UnsupportedCompiler,
            anyhow!("{kind:?}"),
        ));
    }
    if parsed_args.language_after_input {
        return Err(CompileLocallyReason::LanguageAfterInput.into());
    }
    let preprocessed_language = match parsed_args.language {
        Language::C | Language::Cxx => parsed_args.language.to_c_preprocessed_language(),
        _ => None,
    }
    .and_then(Language::to_gcc_arg)
    .ok_or_else(|| {
        NotEligible::new(
            CompileLocallyReason::UnsupportedLanguage,
            anyhow!("{}", parsed_args.language.as_str()),
        )
    })?;
    if let Some((name, _)) = env_vars
        .iter()
        .find(|(name, _)| COMPILER_ENV_VARS.iter().any(|var| name == var))
    {
        return Err(NotEligible::new(
            CompileLocallyReason::CompilerEnvVar,
            anyhow!("{}", name.to_string_lossy()),
        ));
    }
    let path_mapper = path_mapper(cwd, env_vars)?;
    check_options(parsed_args)?;
    if let Some(name) = parsed_args
        .outputs
        .keys()
        .find(|name| !matches!(**name, "obj" | "d"))
    {
        return Err(NotEligible::new(
            CompileLocallyReason::ExtraOutput,
            anyhow!("{name}"),
        ));
    }
    let input = cwd.join(&parsed_args.input);
    let mapped_input = path_mapper.to_input_root(&input).ok_or_else(|| {
        NotEligible::new(
            CompileLocallyReason::SourceOutsideInputRoot,
            anyhow!("{}", input.display()),
        )
    })?;
    let object = cwd.join(&parsed_args.outputs["obj"].path);
    let mapped_object = path_mapper.to_input_root(&object).ok_or_else(|| {
        NotEligible::new(
            CompileLocallyReason::PathOutsideInputRoot,
            anyhow!("{}", object.display()),
        )
    })?;

    let mut arguments = vec![
        "-x".to_owned(),
        preprocessed_language.to_owned(),
        "-c".to_owned(),
        mapped_input.clone(),
        "-o".to_owned(),
        mapped_object,
    ];
    if *kind == CCompilerKind::Gcc {
        arguments.push("-fpreprocessed".to_owned());
    }
    let mut common_args = parsed_args.common_args.iter();
    while let Some(arg) = common_args.next() {
        if arg == "--sysroot" {
            common_args.next();
        } else if !LEFT_OUT_PREFIXES
            .iter()
            .any(|prefix| arg.as_encoded_bytes().starts_with(prefix.as_bytes()))
        {
            arguments.push(map_option(arg, &path_mapper)?);
        }
    }
    Ok(MappedCompile {
        path_mapper,
        arguments,
        mapped_input,
        object,
    })
}

/// The key of `executable`'s toolchain for a `language` compile, or why it runs locally. Runs
/// the compiler, so call it from a blocking thread.
pub(crate) fn toolchain_key(
    kind: &CCompilerKind,
    executable: &Path,
    language: &Language,
    cwd: &Path,
    env_vars: &[(OsString, OsString)],
) -> Result<CToolchainKey, NotEligible> {
    // gcc finds its internal programs relative to its canonical path, so a worker runs the
    // driver from there too.
    let driver = executable
        .canonicalize()
        .unwrap_or_else(|_| executable.to_owned());
    // The worker's command names the driver by its path.
    if driver.to_str().is_none() {
        return Err(NotEligible::new(
            CompileLocallyReason::NonUtf8ToolchainPath,
            anyhow!("{}", driver.display()),
        ));
    }
    let mut internal_files = vec![];
    // clang assembles and compiles in its own process.
    if *kind == CCompilerKind::Gcc {
        let compiler_proper = match language {
            Language::Cxx => "-print-prog-name=cc1plus",
            _ => "-print-prog-name=cc1",
        };
        for option in GCC_INTERNAL_FILES {
            match internal_file(&driver, option, cwd, env_vars) {
                Some(file) => internal_files.push(file),
                // Without its own, a worker would run its gcc's or the one on its `PATH`.
                None if [compiler_proper, "-print-prog-name=as"].contains(option) => {
                    return Err(NotEligible::new(
                        CompileLocallyReason::MissingGccProgram,
                        anyhow!("{option}"),
                    ));
                }
                None => {}
            }
        }
    }
    CToolchainKey::new(kind.clone(), driver, internal_files)
        .map_err(|e| NotEligible::new(CompileLocallyReason::SharedLibrariesNotListed, e))
}

/// The file `driver` prints for `option`, or `None` if it prints none. gcc prints a bare name
/// for a program it doesn't find in its own directories, and then runs the one on `PATH`.
fn internal_file(
    driver: &Path,
    option: &str,
    cwd: &Path,
    env_vars: &[(OsString, OsString)],
) -> Option<InternalFile> {
    let output = std::process::Command::new(driver)
        .arg(option)
        .env_clear()
        .envs(env_vars.iter().cloned())
        .current_dir(cwd)
        .output()
        .ok()
        .filter(|output| output.status.success())?;
    let printed = std::str::from_utf8(&output.stdout).ok()?.trim_end();
    let mut file = InternalFile {
        option: option.to_owned(),
        path: lexically_normalize(Path::new(printed)),
    };
    if !printed.contains('/') {
        if !file.is_program() {
            return None;
        }
        let path_var = env_vars.iter().find(|(name, _)| name == "PATH")?;
        file.path = which::which_in(printed, Some(&path_var.1), cwd).ok()?;
    }
    file.path.is_file().then_some(file)
}

/// Fails for a clang that reads a config file when run as `executable` or as `driver`. The
/// toolchain dir holds none, so a worker's clang wouldn't read it.
fn check_clang_config(
    executable: &Path,
    driver: &Path,
    cwd: &Path,
    env_vars: &[(OsString, OsString)],
) -> Result<(), NotEligible> {
    for program in [executable, driver] {
        let output = std::process::Command::new(program)
            .args(["-###", "-c", "-x", "c", "/dev/null", "-o", "/dev/null"])
            .env_clear()
            .envs(env_vars.iter().cloned())
            .current_dir(cwd)
            .output()
            .map_err(|e| NotEligible::new(CompileLocallyReason::ClangConfigFile, e.into()))?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        if let Some(line) = stderr
            .lines()
            .find(|line| line.starts_with("Configuration file:"))
        {
            return Err(NotEligible::new(
                CompileLocallyReason::ClangConfigFile,
                anyhow!("{line}"),
            ));
        }
    }
    Ok(())
}

/// The `PathMapper` of a build script's compile, whose cwd is the package's manifest dir, and
/// whose build root holds `OUT_DIR` as `<build root>/<profile>/build/<package>/out`.
fn path_mapper(cwd: &Path, env_vars: &[(OsString, OsString)]) -> Result<PathMapper, NotEligible> {
    let dir_var = |name: &str| {
        env_vars
            .iter()
            .find(|(var, _)| var == name)
            .map(|(_, dir)| lexically_normalize(&cwd.join(dir)))
    };
    let (Some(out_dir), Some(manifest_dir)) = (dir_var("OUT_DIR"), dir_var("CARGO_MANIFEST_DIR"))
    else {
        return Err(CompileLocallyReason::NotFromBuildScript.into());
    };
    let cwd = lexically_normalize(cwd);
    if cwd != manifest_dir {
        return Err(NotEligible::new(
            CompileLocallyReason::UnexpectedCargoLayout,
            anyhow!("cwd {} isn't CARGO_MANIFEST_DIR", cwd.display()),
        ));
    }
    let build_root = (out_dir.file_name().is_some_and(|name| name == "out")
        && out_dir
            .ancestors()
            .nth(2)
            .and_then(Path::file_name)
            .is_some_and(|name| name == "build"))
    .then(|| out_dir.ancestors().nth(4))
    .flatten()
    .map(PathBuf::from)
    .ok_or_else(|| {
        NotEligible::new(
            CompileLocallyReason::UnexpectedCargoLayout,
            anyhow!(
                "OUT_DIR {} isn't <profile>/build/<package>/out",
                out_dir.display()
            ),
        )
    })?;
    // A build root holding the cwd would map the cwd's paths under BUILD_ROOT_DIR, so they'd
    // depend on where the checkout lives.
    if cwd.starts_with(&build_root) {
        return Err(NotEligible::new(
            CompileLocallyReason::UnexpectedCargoLayout,
            anyhow!("build root {} holds the cwd", build_root.display()),
        ));
    }
    Ok(PathMapper::new(cwd, build_root))
}

/// Fails for an option whose compile a worker can't match: one that builds for the client's
/// CPU, reads files besides the source, writes files besides the object, records local paths,
/// macros or the command line, or changes the programs the compiler runs.
fn check_options(parsed_args: &ParsedArguments) -> Result<(), NotEligible> {
    if !parsed_args.arch_args.is_empty() {
        return Err(CompileLocallyReason::ArchOption.into());
    }
    if parsed_args.profile_generate {
        return Err(CompileLocallyReason::ProfilingOrCoverage.into());
    }
    let options = parsed_args
        .preprocessor_args
        .iter()
        .chain(&parsed_args.common_args)
        .chain(&parsed_args.unhashed_args);
    for option in options {
        let Some(option) = option.to_str() else {
            return Err(CompileLocallyReason::NonUtf8Argument.into());
        };
        let reason = match option {
            _ if matches!(
                option.split_once('='),
                Some(("-march" | "-mcpu" | "-mtune", cpu)) if cpu.starts_with("native")
            ) =>
            {
                CompileLocallyReason::NativeCpu
            }
            "-include-pch" | "-fpch-preprocess" => CompileLocallyReason::PrecompiledHeader,
            "--coverage" | "-ftest-coverage" | "-fcoverage-mapping" => {
                CompileLocallyReason::ProfilingOrCoverage
            }
            _ if option.starts_with("-fprofile-") => CompileLocallyReason::ProfilingOrCoverage,
            "-g3" | "-ggdb3" | "-fdebug-macro" => CompileLocallyReason::MacroDebugInfo,
            "-frecord-command-line" | "-grecord-command-line" | "-frecord-gcc-switches" => {
                CompileLocallyReason::RecordedCommandLine
            }
            _ if option.starts_with("-f") && option.contains("prefix-map=") => {
                CompileLocallyReason::PrefixMapOption
            }
            _ if option.starts_with("-fplugin")
                || option.starts_with("-fpass-plugin")
                || option.starts_with("-specs")
                || option.starts_with("-fauto-profile")
                || (option.starts_with("-fsanitize") && option.contains("list=")) =>
            {
                CompileLocallyReason::OptionReadsFile
            }
            // A worker's action declares only the object as an output.
            "-fstack-usage" | "-aux-info" | "-MJ" | "-fdiagnostics-format=sarif-file" => {
                CompileLocallyReason::SideOutputOption
            }
            _ if SIDE_OUTPUT_PREFIXES
                .iter()
                .any(|prefix| option.starts_with(prefix))
                || (option.starts_with("-fopt-info") && option.contains('=')) =>
            {
                CompileLocallyReason::SideOutputOption
            }
            "-Xclang" | "-fno-integrated-as" | "-no-integrated-as" | "-wrapper" => {
                CompileLocallyReason::CompilerInternalsOption
            }
            _ if option.starts_with("-B") => CompileLocallyReason::CompilerInternalsOption,
            "-v" | "--verbose" | "-###" => CompileLocallyReason::VerboseOutput,
            _ => continue,
        };
        return Err(NotEligible::new(reason, anyhow!("{option}")));
    }
    if let Some(file) = parsed_args.extra_hash_files.first() {
        return Err(NotEligible::new(
            CompileLocallyReason::OptionReadsFile,
            anyhow!("{}", file.display()),
        ));
    }
    Ok(())
}

/// `option` with an absolute path in it, alone or after `=`, mapped into the input root. An
/// absolute path elsewhere, in a list for another program or right after a flag, isn't.
fn map_option(option: &OsString, path_mapper: &PathMapper) -> Result<String, NotEligible> {
    let option = option
        .to_str()
        .ok_or(NotEligible::from(CompileLocallyReason::NonUtf8Argument))?;
    let not_mapped = || {
        NotEligible::new(
            CompileLocallyReason::AbsolutePathInOption,
            anyhow!("{option}"),
        )
    };
    if option.contains(',') {
        return match option.split([',', '=']).any(is_absolute_path_arg) {
            true => Err(not_mapped()),
            false => Ok(option.to_owned()),
        };
    }
    let (flag, path) = match option.split_once('=') {
        Some((flag, path)) if path.starts_with('/') => (Some(flag), path),
        _ if option.starts_with('/') => (None, option),
        _ if is_absolute_path_arg(option) => return Err(not_mapped()),
        _ => return Ok(option.to_owned()),
    };
    let mapped = path_mapper
        .to_input_root(Path::new(path))
        .ok_or_else(not_mapped)?;
    Ok(match flag {
        Some(flag) => format!("{flag}={mapped}"),
        None => mapped,
    })
}

/// Whether `arg` is an absolute path, or a flag followed by one, as in `-T/x.ld`.
fn is_absolute_path_arg(arg: &str) -> bool {
    let path = arg.strip_prefix('-').map_or(arg, |flag| {
        flag.trim_start_matches(|c: char| c.is_ascii_alphabetic())
    });
    path.starts_with('/')
}

/// Preprocesses the compile of `parsed_args` again for a worker, into a `.i` file under `dir`,
/// and writes its depfile. sccache's own preprocessing can't be sent: it leaves out line
/// markers and keeps local paths in `__FILE__`. Runs the compiler, so call it from a blocking
/// thread.
fn preprocess_for_reapi(
    kind: &CCompilerKind,
    parsed_args: &ParsedArguments,
    executable: &Path,
    cwd: &Path,
    env_vars: &[(OsString, OsString)],
    path_mapper: &PathMapper,
    dir: &Path,
) -> Result<TempPath, NotEligible> {
    let output = std::process::Command::new(executable)
        .args(preprocess_args(kind, parsed_args, path_mapper))
        .env_clear()
        .envs(env_vars.iter().cloned())
        .current_dir(cwd)
        .output()
        .map_err(|e| NotEligible::new(CompileLocallyReason::PreprocessingFailed, e.into()))?;
    if !output.status.success() {
        return Err(NotEligible::new(
            CompileLocallyReason::PreprocessingFailed,
            anyhow!(
                "{}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            ),
        ));
    }
    let preprocessed = rewrite_line_markers(&output.stdout, path_mapper, kind);
    let written = std::fs::create_dir_all(dir).and_then(|()| {
        let mut file = tempfile::Builder::new().suffix(".i").tempfile_in(dir)?;
        file.write_all(&preprocessed)?;
        Ok(file.into_temp_path())
    });
    written
        .map_err(|e| NotEligible::new(CompileLocallyReason::PreprocessedSourceNotWritten, e.into()))
}

/// The arguments that preprocess the compile of `parsed_args` to stdout, with `__FILE__`
/// naming paths as a worker's compile does.
fn preprocess_args(
    kind: &CCompilerKind,
    parsed_args: &ParsedArguments,
    path_mapper: &PathMapper,
) -> Vec<OsString> {
    let mut args = super::gcc::preprocess_args(
        parsed_args,
        &parsed_args.arch_args,
        true,
        kind.clone(),
        false,
        &[],
        Language::to_gcc_arg,
    );
    // After a `--`, the prefix maps would be inputs.
    let input_at = args.len() - 1 - usize::from(parsed_args.double_dash_input);
    let prefix_maps = path_mapper.prefix_map_args("-fmacro-prefix-map=");
    args.splice(input_at..input_at, prefix_maps);
    args
}

/// `preprocessed` with the paths under the cwd and build root in its line markers written as
/// a worker's compile names them. A path that `kind` escapes differently from
/// [`escape_line_marker_path`] stays local, which costs only cache hits across checkouts.
fn rewrite_line_markers(
    preprocessed: &[u8],
    path_mapper: &PathMapper,
    kind: &CCompilerKind,
) -> Vec<u8> {
    // The build root, often inside the cwd, is matched first.
    let prefixes: Vec<(Vec<u8>, &str)> = path_mapper
        .input_root_prefixes()
        .into_iter()
        .rev()
        .map(|(local, mapped)| {
            let local = escape_line_marker_path(local.as_os_str().as_encoded_bytes(), kind);
            (local, mapped)
        })
        .collect();
    let mut rewritten = Vec::with_capacity(preprocessed.len());
    let mut start = 0;
    while start < preprocessed.len() {
        let end = memchr::memchr(b'\n', &preprocessed[start..])
            .map_or(preprocessed.len(), |newline| start + newline + 1);
        let line = &preprocessed[start..end];
        let path_start = super::c::is_line_marker_at(preprocessed, start)
            .then(|| memchr::memchr(b'"', line))
            .flatten()
            .map(|quote| quote + 1);
        let mapped = path_start.and_then(|path_start| {
            prefixes.iter().find_map(|(local, mapped)| {
                let rest = line[path_start..].strip_prefix(local.as_slice())?;
                matches!(rest.first(), Some(b'/' | b'"')).then_some((path_start, *mapped, rest))
            })
        });
        match mapped {
            Some((path_start, mapped, rest)) => {
                rewritten.extend_from_slice(&line[..path_start]);
                rewritten.extend_from_slice(mapped.as_bytes());
                rewritten.extend_from_slice(rest);
            }
            None => rewritten.extend_from_slice(line),
        }
        start = end;
    }
    rewritten
}

/// `path` as `kind`'s preprocessor writes it between the quotes of a line marker.
fn escape_line_marker_path(path: &[u8], kind: &CCompilerKind) -> Vec<u8> {
    let mut escaped = Vec::with_capacity(path.len());
    for &byte in path {
        match byte {
            b'\\' | b'"' => escaped.extend_from_slice(&[b'\\', byte]),
            b'\n' => escaped.extend_from_slice(b"\\n"),
            b'\t' if *kind == CCompilerKind::Clang => escaped.extend_from_slice(b"\\t"),
            // clang writes every other byte that isn't printable ASCII, UTF-8 included, in
            // octal.
            _ if *kind == CCompilerKind::Clang && !(b' '..=b'~').contains(&byte) => {
                escaped.extend_from_slice(format!("\\{byte:03o}").as_bytes());
            }
            _ => escaped.push(byte),
        }
    }
    escaped
}

#[cfg(all(test, unix))]
mod test {
    use super::*;
    use crate::compiler::CompilerArguments;
    use crate::compiler::{clang, gcc};
    use crate::test::utils::write_executable;
    use CCompilerKind::{Clang, Diab, Gcc};
    use CompileLocallyReason as R;
    use test_case::test_case;

    const WS: &str = "/ws/app";
    const OUT_DIR: &str = "/ws/target/debug/build/foo-1/out";
    const OBJECT: &str = "/ws/target/debug/build/foo-1/out/x.o";
    const BUILD_SCRIPT_ENV: &[(&str, &str)] = &[("OUT_DIR", OUT_DIR), ("CARGO_MANIFEST_DIR", WS)];

    fn parse(kind: &CCompilerKind, cwd: &Path, args: &[&str]) -> ParsedArguments {
        let args: Vec<OsString> = args.iter().map(OsString::from).collect();
        let kind = kind.clone();
        let parsed = match kind {
            CCompilerKind::Clang => {
                gcc::parse_arguments(&args, cwd, (&gcc::ARGS[..], &clang::ARGS[..]), false, kind)
            }
            _ => gcc::parse_arguments(&args, cwd, &gcc::ARGS[..], false, kind),
        };
        match parsed {
            CompilerArguments::Ok(parsed) => parsed,
            other => panic!("{other:?}"),
        }
    }

    fn env_vars(vars: &[(&str, &str)]) -> Vec<(OsString, OsString)> {
        vars.iter()
            .map(|(name, value)| (name.into(), value.into()))
            .collect()
    }

    /// A `kind` compile in `cwd` with `args`, run with `env`.
    fn map_args(
        kind: &CCompilerKind,
        cwd: &str,
        args: &[&str],
        env: &[(&str, &str)],
    ) -> Result<MappedCompile, NotEligible> {
        let parsed_args = parse(kind, Path::new(cwd), args);
        map_compile(kind, &parsed_args, Path::new(cwd), &env_vars(env))
    }

    /// A `kind` compile in `ws` of `src/x.c` into `out_dir`, with `args` added.
    fn map_in(
        kind: &CCompilerKind,
        ws: &str,
        out_dir: &str,
        args: &[&str],
        env: &[(&str, &str)],
    ) -> Result<MappedCompile, NotEligible> {
        let object = format!("{out_dir}/x.o");
        let mut all_args = args.to_vec();
        all_args.extend(["-c", "src/x.c", "-o", object.as_str()]);
        map_args(kind, ws, &all_args, env)
    }

    /// A `kind` compile of `src/x.c` by a build script, with `args` added.
    fn map(kind: &CCompilerKind, args: &[&str]) -> Result<MappedCompile, NotEligible> {
        map_in(kind, WS, OUT_DIR, args, BUILD_SCRIPT_ENV)
    }

    fn reason<T>(mapped: Result<T, NotEligible>) -> CompileLocallyReason {
        mapped.err().expect("the compile mapped").reason
    }

    #[test_case(Diab, &[], R::UnsupportedCompiler ; "other compiler")]
    #[test_case(Gcc, &["-x", "assembler"], R::UnsupportedLanguage ; "assembler")]
    #[test_case(Gcc, &["-x", "c-header"], R::UnsupportedLanguage ; "header")]
    #[test_case(Gcc, &["-march=native"], R::NativeCpu ; "native arch")]
    #[test_case(Gcc, &["-mtune=native"], R::NativeCpu ; "native tune")]
    #[test_case(Gcc, &["-mcpu=native+nosve"], R::NativeCpu ; "native cpu with features")]
    #[test_case(Gcc, &["--coverage"], R::ProfilingOrCoverage ; "coverage")]
    #[test_case(Gcc, &["-fprofile-generate"], R::ProfilingOrCoverage ; "profile")]
    #[test_case(Clang, &["-fprofile-use=x.prof"], R::ProfilingOrCoverage ; "pgo")]
    #[test_case(Gcc, &["-g3"], R::MacroDebugInfo ; "g3")]
    #[test_case(Clang, &["-fdebug-macro"], R::MacroDebugInfo ; "debug macro")]
    #[test_case(Gcc, &["-frecord-gcc-switches"], R::RecordedCommandLine ; "gcc switches")]
    #[test_case(Gcc, &["-frecord-command-line"], R::RecordedCommandLine ; "command line")]
    #[test_case(Clang, &["-grecord-command-line"], R::RecordedCommandLine ; "debug command line")]
    #[test_case(Gcc, &["-ffile-prefix-map=/ws=."], R::PrefixMapOption ; "file map")]
    #[test_case(Gcc, &["-fdebug-prefix-map=/a=b"], R::PrefixMapOption ; "debug map")]
    #[test_case(Clang, &["-include-pch", "x.pch"], R::PrecompiledHeader ; "pch")]
    #[test_case(Clang, &["-fplugin=x.so"], R::OptionReadsFile ; "plugin")]
    #[test_case(Clang, &["-fsanitize-ignorelist=x.txt"], R::OptionReadsFile ; "sanitizer list")]
    #[test_case(Gcc, &["-fauto-profile=x.afdo"], R::OptionReadsFile ; "auto profile")]
    #[test_case(Gcc, &["-fdump-tree-all"], R::SideOutputOption ; "dump")]
    #[test_case(Gcc, &["-aux-info", "x.h"], R::SideOutputOption ; "aux info")]
    #[test_case(Gcc, &["-fopt-info-vec=x.txt"], R::SideOutputOption ; "opt info file")]
    #[test_case(Gcc, &["-fdiagnostics-format=sarif-file"], R::SideOutputOption ; "sarif file")]
    #[test_case(Gcc, &["-fdiagnostics-add-output=sarif"], R::SideOutputOption ; "added output")]
    #[test_case(Gcc, &["-B/opt/bin"], R::CompilerInternalsOption ; "B")]
    #[test_case(Gcc, &["-fno-integrated-as"], R::CompilerInternalsOption ; "no as")]
    #[test_case(Clang, &["-Xclang", "-fno-pch-timestamp"], R::CompilerInternalsOption ; "xclang")]
    #[test_case(Gcc, &["-v"], R::VerboseOutput ; "verbose")]
    #[test_case(Clang, &["-arch", "x86_64"], R::ArchOption ; "arch")]
    fn test_compiles_that_stay_local(
        kind: CCompilerKind,
        args: &[&str],
        expected: CompileLocallyReason,
    ) {
        assert_eq!(expected, reason(map(&kind, args)));
    }

    #[test]
    fn test_compiler_env_vars_stay_local() {
        let env = [
            ("GCC_EXEC_PREFIX", "/opt/gcc/"),
            BUILD_SCRIPT_ENV[0],
            BUILD_SCRIPT_ENV[1],
        ];
        let mapped = map_in(&CCompilerKind::Gcc, WS, OUT_DIR, &[], &env);
        assert_eq!(CompileLocallyReason::CompilerEnvVar, reason(mapped));
    }

    #[test]
    fn test_a_language_after_the_input_stays_local() {
        let args = ["-c", "src/x.cpp", "-o", "x.o", "-x", "c"];
        let mapped = map_args(&CCompilerKind::Gcc, WS, &args, BUILD_SCRIPT_ENV);
        assert_eq!(CompileLocallyReason::LanguageAfterInput, reason(mapped));
    }

    #[test_case(WS, &[], CompileLocallyReason::NotFromBuildScript ; "not from a build script")]
    // As in cmake-crate builds, which run the compiler under OUT_DIR.
    #[test_case(OUT_DIR, BUILD_SCRIPT_ENV, CompileLocallyReason::UnexpectedCargoLayout ;
        "cwd other than the manifest dir")]
    #[test_case(WS, &[("OUT_DIR", "/ws/out"), BUILD_SCRIPT_ENV[1]],
        CompileLocallyReason::UnexpectedCargoLayout ; "OUT_DIR outside a build dir")]
    #[test_case(WS, &[("OUT_DIR", "/debug/build/foo-1/out"), BUILD_SCRIPT_ENV[1]],
        CompileLocallyReason::UnexpectedCargoLayout ; "build root holding the cwd")]
    #[test_case("/ws/other", BUILD_SCRIPT_ENV, CompileLocallyReason::UnexpectedCargoLayout ;
        "cwd outside the manifest dir and build root")]
    fn test_compiles_outside_a_cargo_build_script_stay_local(
        cwd: &str,
        env: &[(&str, &str)],
        expected: CompileLocallyReason,
    ) {
        let mapped = map_in(&CCompilerKind::Gcc, cwd, OUT_DIR, &[], env);
        assert_eq!(expected, reason(mapped));
    }

    #[test_case(&["-c", "/elsewhere/x.c", "-o", "x.o"],
        CompileLocallyReason::SourceOutsideInputRoot ; "source outside the roots")]
    #[test_case(&["-c", "src/x.c", "-o", "/elsewhere/x.o"],
        CompileLocallyReason::PathOutsideInputRoot ; "object outside the roots")]
    #[test_case(&["-c", "src/x.c", "-o", OBJECT, "-gsplit-dwarf"],
        CompileLocallyReason::ExtraOutput ; "dwo output")]
    #[test_case(&["-c", "src/x.c", "-o", OBJECT, "-fexample-dir=/opt/x"],
        CompileLocallyReason::AbsolutePathInOption ; "absolute path outside the roots")]
    #[test_case(&["-c", "src/x.c", "-o", OBJECT, "-Wa,-I/ws/app/include"],
        CompileLocallyReason::AbsolutePathInOption ; "absolute path in a list")]
    #[test_case(&["-c", "src/x.c", "-o", OBJECT, "-T/ws/app/x.ld"],
        CompileLocallyReason::AbsolutePathInOption ; "absolute path after a flag")]
    fn test_sources_and_outputs_that_stay_local(args: &[&str], expected: CompileLocallyReason) {
        let mapped = map_args(&CCompilerKind::Gcc, WS, args, BUILD_SCRIPT_ENV);
        assert_eq!(expected, reason(mapped));
    }

    #[test]
    fn test_arguments_drop_preprocessor_options_and_map_paths() {
        let compile = map(
            &CCompilerKind::Gcc,
            &[
                "-O2",
                "-Isrc/include",
                "-DFOO=1",
                "-MD",
                "-MF",
                "/ws/target/debug/build/foo-1/out/x.d",
                "-L/ws/target/lib",
                "-Wl,--as-needed",
                "-fexample-dir=/ws/app/data",
                "-Wa,--noexecstack",
            ],
        )
        .unwrap();
        assert_eq!(
            [
                "-x",
                "cpp-output",
                "-c",
                "src/x.c",
                "-o",
                ".sccache-build/debug/build/foo-1/out/x.o",
                "-fpreprocessed",
                "-O2",
                "-fexample-dir=data",
                "-Wa,--noexecstack",
            ],
            compile.arguments.as_slice()
        );
        assert_eq!(Path::new(OBJECT), compile.object);
    }

    #[test_case(&["--sysroot", "/opt/x"] ; "separate")]
    #[test_case(&["--sysroot=/opt/x"] ; "joined")]
    fn test_cross_compiles_leave_out_the_sysroot(sysroot: &[&str]) {
        let mut args = vec!["--target=aarch64-unknown-linux-gnu"];
        args.extend(sysroot);
        let compile = map(&CCompilerKind::Clang, &args).unwrap();
        assert_eq!(
            ["--target=aarch64-unknown-linux-gnu"],
            compile.arguments[6..]
        );
    }

    #[test]
    fn test_clang_compiles_cpp_output_without_fpreprocessed() {
        let compile = map(&CCompilerKind::Clang, &["-x", "c++"]).unwrap();
        assert_eq!(
            ["-x", "c++-cpp-output", "-c", "src/x.c"],
            compile.arguments[..4]
        );
        assert!(!compile.arguments.contains(&"-fpreprocessed".to_owned()));
    }

    /// A directory holding a gcc driver that prints `<dir>/cc1` for cc1 and only the name of
    /// the other programs, of which `as` is in `<dir>/bin`, and the env vars with that dir on
    /// `PATH`.
    fn fake_gcc(dir: &Path) -> (PathBuf, Vec<(OsString, OsString)>) {
        let dir = dir.canonicalize().unwrap();
        let driver = dir.join("gcc-12");
        let script = format!(
            "case \"$1\" in\n-print-prog-name=cc1) echo {}/cc1 ;;\n\
             *) echo \"${{1#*=}}\" ;;\nesac\n",
            dir.display()
        );
        write_executable(&driver, &script);
        write_executable(&dir.join("cc1"), "");
        write_executable(&dir.join("bin/as"), "");
        std::os::unix::fs::symlink(&driver, dir.join("cc")).unwrap();
        let env = vec![("PATH".into(), dir.join("bin").into_os_string())];
        (dir, env)
    }

    fn internal_file(option: &str, path: PathBuf) -> InternalFile {
        InternalFile {
            option: option.to_owned(),
            path,
        }
    }

    #[test]
    fn test_gcc_key() {
        let dir = tempfile::tempdir().unwrap();
        let (dir, env) = fake_gcc(dir.path());
        let key = || toolchain_key(&Gcc, &dir.join("cc"), &Language::C, &dir, &env).unwrap();
        let original = key();
        assert_eq!(dir.join("gcc-12"), original.driver);
        assert_eq!(
            [
                internal_file("-print-prog-name=cc1", dir.join("cc1")),
                internal_file("-print-prog-name=as", dir.join("bin/as")),
            ],
            original.internal_files.as_slice()
        );
        assert_eq!(original, key());

        std::fs::write(dir.join("bin/as"), "a newer as").unwrap();
        assert_ne!(original, key(), "as changed");
    }

    #[test_case(Language::C, "bin/as", "-print-prog-name=as" ; "assembler")]
    #[test_case(Language::C, "cc1", "-print-prog-name=cc1" ; "cc1")]
    #[test_case(Language::Cxx, "", "-print-prog-name=cc1plus" ; "cc1plus")]
    fn test_a_gcc_without_the_programs_it_runs_stays_local(
        language: Language,
        removed: &str,
        option: &str,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let (dir, env) = fake_gcc(dir.path());
        if !removed.is_empty() {
            std::fs::remove_file(dir.join(removed)).unwrap();
        }
        let not_eligible = toolchain_key(&Gcc, &dir.join("cc"), &language, &dir, &env).unwrap_err();
        assert_eq!(R::MissingGccProgram, not_eligible.reason);
        assert_eq!(option, not_eligible.detail.unwrap().to_string());
    }

    #[test]
    fn test_a_clang_key_has_no_internal_files() {
        let dir = tempfile::tempdir().unwrap();
        let (dir, env) = fake_gcc(dir.path());
        let key = toolchain_key(&Clang, &dir.join("cc"), &Language::C, &dir, &env).unwrap();
        assert!(key.internal_files.is_empty(), "{key:?}");
    }

    #[test_case("*/cc", true ; "as the executable")]
    #[test_case("*/clang-18", true ; "as the driver")]
    #[test_case("*/other", false ; "neither")]
    fn test_a_clang_that_reads_a_config_file_stays_local(reads_config_as: &str, local: bool) {
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path().canonicalize().unwrap();
        let script = format!(
            "case \"$0\" in {reads_config_as}) echo 'Configuration file: /x.cfg' >&2 ;; esac\n"
        );
        write_executable(&dir.join("clang-18"), &script);
        std::os::unix::fs::symlink(dir.join("clang-18"), dir.join("cc")).unwrap();
        let checked = check_clang_config(&dir.join("cc"), &dir.join("clang-18"), &dir, &[]);
        assert_eq!(local, checked.is_err(), "{checked:?}");
        if let Err(not_eligible) = checked {
            assert_eq!(R::ClangConfigFile, not_eligible.reason);
        }
    }

    #[test]
    fn test_two_checkouts_get_the_same_arguments() {
        let map_in_checkout = |ws: &str, target: &str| {
            let out_dir = format!("{target}/x86_64-unknown-linux-gnu/release/build/foo-1/out");
            let env = [("OUT_DIR", out_dir.as_str()), ("CARGO_MANIFEST_DIR", ws)];
            let include = format!("{out_dir}/include");
            map_in(
                &CCompilerKind::Gcc,
                ws,
                &out_dir,
                &["-O2", "-I", &include],
                &env,
            )
            .unwrap()
            .arguments
        };
        assert_eq!(
            map_in_checkout("/home/a/ws/foo", "/home/a/ws/target"),
            map_in_checkout(
                "/home/b/.cargo/registry/src/index-1/foo-1.0.0",
                "/srv/b/target"
            )
        );
    }

    #[test]
    fn test_line_markers_name_paths_as_a_worker_does() {
        let path_mapper = PathMapper::new("/w\"s".into(), "/w\"s/target".into());
        let preprocessed = br#"# 1 "/w\"s/src/x.c"
# 1 "/w\"s//"
# 5 "/w\"s/target/out/gen.h" 1
#line 2 "/w\"sx/y.h"
const char *s = "/w\"s/src/x.c";
# 3 "/usr/include/stdio.h" 3 4
"#;
        let expected = br#"# 1 "./src/x.c"
# 1 ".//"
# 5 ".sccache-build/out/gen.h" 1
#line 2 "/w\"sx/y.h"
const char *s = "/w\"s/src/x.c";
# 3 "/usr/include/stdio.h" 3 4
"#;
        let rewritten = rewrite_line_markers(preprocessed, &path_mapper, &CCompilerKind::Gcc);
        assert_eq!(
            String::from_utf8_lossy(expected),
            String::from_utf8_lossy(&rewritten)
        );
    }

    #[test]
    fn test_clang_escapes_non_ascii_paths_in_octal() {
        assert_eq!(
            br#"/caf\303\251/\"x\""#.as_slice(),
            escape_line_marker_path("/caf\u{e9}/\"x\"".as_bytes(), &CCompilerKind::Clang)
        );
        assert_eq!(
            "/caf\u{e9}/\\\"x\\\"".as_bytes(),
            escape_line_marker_path("/caf\u{e9}/\"x\"".as_bytes(), &CCompilerKind::Gcc)
        );
    }

    #[test]
    fn test_prefix_maps_come_before_a_double_dash() {
        let kind = &CCompilerKind::Clang;
        let parsed_args = parse(kind, Path::new(WS), &["-c", "-o", OBJECT, "--", "src/x.c"]);
        assert!(parsed_args.double_dash_input);
        let path_mapper = PathMapper::new(WS.into(), "/ws/target".into());
        let args = preprocess_args(kind, &parsed_args, &path_mapper);
        assert_eq!(
            [
                "-fmacro-prefix-map=/ws/app=.",
                "-fmacro-prefix-map=/ws/target=.sccache-build",
                "--",
                "src/x.c"
            ],
            args[args.len() - 4..]
        );
    }

    /// `gcc` or `clang` for `kind`, or `None` when that program isn't installed or runs another
    /// compiler, as `gcc` does on macOS.
    fn compiler(kind: &CCompilerKind) -> Option<&'static str> {
        let (program, version) = match kind {
            CCompilerKind::Gcc => ("gcc", "Free Software Foundation"),
            _ => ("clang", "clang version"),
        };
        let output = std::process::Command::new(program)
            .arg("--version")
            .output();
        let installed =
            output.is_ok_and(|output| String::from_utf8_lossy(&output.stdout).contains(version));
        if !installed {
            eprintln!("No {kind:?} found as {program}, skipping test");
        }
        installed.then_some(program)
    }

    /// A compile in a checkout at `ws` of a `src/x.c` that includes `src/x.h` and an `OUT_DIR`
    /// header, each naming `__FILE__`.
    struct Checkout {
        parsed_args: ParsedArguments,
        driver: PathBuf,
        cwd: PathBuf,
        env_vars: Vec<(OsString, OsString)>,
    }

    impl Checkout {
        /// The compile mapped for REAPI with its preprocessed source under `dir`, as on a
        /// Linux client.
        fn reapi_compile(&self, kind: &CCompilerKind, dir: &Path) -> ReapiCompile {
            let (parsed_args, cwd, env_vars) = (&self.parsed_args, &self.cwd, &self.env_vars);
            let mapped = map_compile(kind, parsed_args, cwd, env_vars).unwrap();
            reapi_compile(mapped, kind, parsed_args, &self.driver, cwd, env_vars, dir).unwrap()
        }

        /// The `.i` that the compile writes under `dir`.
        fn preprocess(&self, kind: &CCompilerKind, dir: &Path) -> Vec<u8> {
            let compile = self.reapi_compile(kind, dir);
            std::fs::read(&compile.preprocessed_source.unwrap().path).unwrap()
        }
    }

    /// The [`Checkout`] at `ws` of a `kind` compile with `args` added, or `None` without
    /// `kind`'s compiler.
    fn checkout(kind: &CCompilerKind, ws: &Path, args: &[&str]) -> Option<Checkout> {
        let driver = PathBuf::from(compiler(kind)?);
        let out_dir = ws.join("target/debug/build/foo-1/out");
        std::fs::create_dir_all(ws.join("src")).unwrap();
        std::fs::create_dir_all(&out_dir).unwrap();
        // The compiler sees its cwd with symlinks resolved, as cargo does.
        let ws = ws.canonicalize().unwrap();
        let out_dir = out_dir.canonicalize().unwrap();
        let file = "const char *file(void) { return __FILE__; }\n";
        std::fs::write(
            ws.join("src/x.c"),
            format!("#include \"x.h\"\n#include \"gen.h\"\n#include <stddef.h>\n{file}"),
        )
        .unwrap();
        std::fs::write(ws.join("src/x.h"), file.replace("file", "header")).unwrap();
        std::fs::write(out_dir.join("gen.h"), file.replace("file", "generated")).unwrap();
        let (source, object) = (ws.join("src/x.c"), out_dir.join("x.o"));
        let mut all_args = vec![
            "-c",
            source.to_str().unwrap(),
            "-o",
            object.to_str().unwrap(),
            "-I",
            out_dir.to_str().unwrap(),
            "-g",
        ];
        all_args.extend(args);
        let parsed_args = parse(kind, &ws, &all_args);
        let (ws_str, out_dir_str) = (ws.to_str().unwrap(), out_dir.to_str().unwrap());
        let mut env_vars = env_vars(&[("OUT_DIR", out_dir_str), ("CARGO_MANIFEST_DIR", ws_str)]);
        env_vars.push(("PATH".into(), std::env::var_os("PATH").unwrap()));
        Some(Checkout {
            parsed_args,
            driver,
            cwd: ws,
            env_vars,
        })
    }

    #[test_case(CCompilerKind::Gcc ; "gcc")]
    #[test_case(CCompilerKind::Clang ; "clang")]
    fn test_mapped_compiles_wait_for_the_c_toolchain(kind: CCompilerKind) {
        let dir = tempfile::tempdir().unwrap();
        let Some(c) = checkout(&kind, &dir.path().join("ws"), &[]) else {
            return;
        };
        let preprocessed = dir.path().join("preprocessed");
        let mapped = map_for_reapi(
            &kind,
            &c.parsed_args,
            &c.driver,
            &c.cwd,
            &c.env_vars,
            &preprocessed,
        );
        assert_eq!(CompileLocallyReason::CToolchainUnsupported, reason(mapped));
    }

    #[test_case(CCompilerKind::Gcc ; "gcc")]
    #[test_case(CCompilerKind::Clang ; "clang")]
    fn test_preprocessed_source_path_and_lifetime(kind: CCompilerKind) {
        let dir = tempfile::tempdir().unwrap();
        let preprocessed = dir.path().join("preprocessed");
        let Some(c) = checkout(&kind, &dir.path().join("ws"), &[]) else {
            return;
        };
        let compile = c.reapi_compile(&kind, &preprocessed);
        let source = compile.preprocessed_source.as_ref().unwrap();
        assert_eq!("src/x.c", source.mapped_path);
        assert!(source.path.starts_with(&preprocessed), "{source:?}");
        assert_eq!(
            Some("src/x.c"),
            compile.arguments.get(3).map(String::as_str)
        );
        let path = source.path.clone();
        drop(compile);
        assert!(!path.exists(), "{path:?} outlived the compile");
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        memchr::memmem::find(haystack, needle).is_some()
    }

    #[test_case(CCompilerKind::Gcc ; "gcc")]
    #[test_case(CCompilerKind::Clang ; "clang")]
    fn test_two_checkouts_preprocess_to_the_same_bytes(kind: CCompilerKind) {
        let dir = tempfile::tempdir().unwrap();
        let preprocessed = dir.path().join("preprocessed");
        let Some(a) = checkout(&kind, &dir.path().join("a/ws"), &[]) else {
            return;
        };
        let a = a.preprocess(&kind, &preprocessed);
        let b = checkout(&kind, &dir.path().join("b/c/ws"), &[]).unwrap();
        let b = b.preprocess(&kind, &preprocessed);
        assert!(a == b, "{}", String::from_utf8_lossy(&a));
        let dir = dir.path().canonicalize().unwrap();
        assert!(!contains(&a, dir.as_os_str().as_encoded_bytes()));
        let generated = ".sccache-build/debug/build/foo-1/out/gen.h";
        for path in ["./src/x.c", "./src/x.h", generated] {
            assert!(contains(&a, format!("# 1 \"{path}\"").as_bytes()), "{path}");
        }
        // Under the cwd, clang's `__FILE__` drops the `./` that gcc keeps, so this checks the
        // build root's.
        let file = format!("generated(void) {{ return \"{generated}\"; }}");
        assert!(contains(&a, file.as_bytes()), "{file}");
    }

    #[test_case(CCompilerKind::Gcc ; "gcc")]
    #[test_case(CCompilerKind::Clang ; "clang")]
    fn test_escaped_paths_are_rewritten(kind: CCompilerKind) {
        let dir = tempfile::tempdir().unwrap();
        let preprocessed = dir.path().join("preprocessed");
        let escaped = dir.path().join("a \"quoted\" \\ws");
        let Some(a) = checkout(&kind, &escaped, &[]) else {
            return;
        };
        let a = a.preprocess(&kind, &preprocessed);
        let b = checkout(&kind, &dir.path().join("ws"), &[]).unwrap();
        let b = b.preprocess(&kind, &preprocessed);
        assert!(a == b, "{}", String::from_utf8_lossy(&a));
    }

    #[test_case(CCompilerKind::Gcc ; "gcc")]
    #[test_case(CCompilerKind::Clang ; "clang")]
    fn test_preprocessing_writes_the_depfile(kind: CCompilerKind) {
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path().canonicalize().unwrap().join("ws");
        let out_dir = ws.join("target/debug/build/foo-1/out");
        let depfile = out_dir.join("x.d");
        let args = ["-MD", "-MF", depfile.to_str().unwrap()];
        let Some(c) = checkout(&kind, &ws, &args) else {
            return;
        };
        c.preprocess(&kind, &dir.path().join("preprocessed"));
        let depfile = std::fs::read_to_string(depfile).unwrap();
        let object = out_dir.join("x.o");
        assert!(
            depfile.starts_with(&format!("{}:", object.display())),
            "{depfile}"
        );
        assert!(
            depfile.contains(&format!(" {}", ws.join("src/x.h").display())),
            "{depfile}"
        );
    }
}
