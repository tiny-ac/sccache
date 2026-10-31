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
    CToolchainKey, CompileLocallyReason, NotEligible, PathMapper, ReapiCompile, ToolchainKey,
    WorkerProgram, lexically_normalize,
};
use anyhow::anyhow;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

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

pub(super) fn map_for_reapi(
    kind: &CCompilerKind,
    parsed_args: &ParsedArguments,
    executable: &Path,
    cwd: &Path,
    env_vars: &[(OsString, OsString)],
) -> Result<ReapiCompile, NotEligible> {
    map_compile(kind, parsed_args, executable, cwd, env_vars)?;
    // Workers can't run gcc or clang without their toolchain in the input root.
    Err(CompileLocallyReason::CToolchainUnsupported.into())
}

/// The compile of `parsed_args` by `executable` in `cwd`, mapped for REAPI, or why it runs
/// locally. Reads the driver's stamp, so call it from a blocking thread.
fn map_compile(
    kind: &CCompilerKind,
    parsed_args: &ParsedArguments,
    executable: &Path,
    cwd: &Path,
    env_vars: &[(OsString, OsString)],
) -> Result<ReapiCompile, NotEligible> {
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
    // The worker's command names the driver by its path.
    if executable.to_str().is_none() {
        return Err(NotEligible::new(
            CompileLocallyReason::NonUtf8ToolchainPath,
            anyhow!("{}", executable.display()),
        ));
    }

    let mut arguments = vec![
        "-x".to_owned(),
        preprocessed_language.to_owned(),
        "-c".to_owned(),
        mapped_input,
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
    Ok(ReapiCompile {
        path_mapper,
        arguments,
        program: WorkerProgram::C,
        toolchain: ToolchainKey::C(CToolchainKey::new(executable.to_owned())),
        env: Default::default(),
        input_root_files: vec![],
        outputs: vec![object],
    })
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
/// CPU, reads files besides the source, records local paths, macros or the command line, or
/// changes the programs the compiler runs.
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
                || (option.starts_with("-fsanitize") && option.contains("list=")) =>
            {
                CompileLocallyReason::OptionReadsFile
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

#[cfg(all(test, unix))]
mod test {
    use super::*;
    use crate::compiler::CompilerArguments;
    use crate::compiler::{clang, gcc};
    use CCompilerKind::{Clang, Diab, Gcc};
    use CompileLocallyReason as R;
    use test_case::test_case;

    const WS: &str = "/ws/app";
    const OUT_DIR: &str = "/ws/target/debug/build/foo-1/out";
    const OBJECT: &str = "/ws/target/debug/build/foo-1/out/x.o";
    const BUILD_SCRIPT_ENV: &[(&str, &str)] = &[("OUT_DIR", OUT_DIR), ("CARGO_MANIFEST_DIR", WS)];
    const CC: &str = "/usr/bin/cc";

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
    ) -> Result<ReapiCompile, NotEligible> {
        let parsed_args = parse(kind, Path::new(cwd), args);
        let (cc, cwd) = (Path::new(CC), Path::new(cwd));
        map_compile(kind, &parsed_args, cc, cwd, &env_vars(env))
    }

    /// A `kind` compile in `ws` of `src/x.c` into `out_dir`, with `args` added.
    fn map_in(
        kind: &CCompilerKind,
        ws: &str,
        out_dir: &str,
        args: &[&str],
        env: &[(&str, &str)],
    ) -> Result<ReapiCompile, NotEligible> {
        let object = format!("{out_dir}/x.o");
        let mut all_args = args.to_vec();
        all_args.extend(["-c", "src/x.c", "-o", object.as_str()]);
        map_args(kind, ws, &all_args, env)
    }

    /// A `kind` compile of `src/x.c` by a build script, with `args` added.
    fn map(kind: &CCompilerKind, args: &[&str]) -> Result<ReapiCompile, NotEligible> {
        map_in(kind, WS, OUT_DIR, args, BUILD_SCRIPT_ENV)
    }

    fn reason(mapped: Result<ReapiCompile, NotEligible>) -> CompileLocallyReason {
        mapped.map(|compile| compile.arguments).unwrap_err().reason
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
        assert_eq!([PathBuf::from(OBJECT)], compile.outputs.as_slice());
        assert!(matches!(compile.program, WorkerProgram::C));
        assert!(
            matches!(&compile.toolchain, ToolchainKey::C(toolchain) if toolchain.driver == Path::new(CC)),
            "{:?}",
            compile.toolchain
        );
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

    #[test]
    fn test_mapped_compiles_wait_for_the_c_toolchain() {
        let kind = &CCompilerKind::Gcc;
        let parsed_args = parse(kind, Path::new(WS), &["-c", "src/x.c", "-o", OBJECT]);
        let (cc, ws) = (Path::new(CC), Path::new(WS));
        let mapped = map_for_reapi(kind, &parsed_args, cc, ws, &env_vars(BUILD_SCRIPT_ENV));
        assert_eq!(CompileLocallyReason::CToolchainUnsupported, reason(mapped));
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
}
