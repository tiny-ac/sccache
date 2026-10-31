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
//! the ones sccache-dist sends: no `-I`, `-D` or `-M` options.

use crate::compiler::Language;
use crate::compiler::c::{CCompilerKind, ParsedArguments};
use crate::reapi::{
    CompileLocallyReason, NotEligible, PathMapper, ReapiCompile, lexically_normalize,
};
use anyhow::anyhow;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

pub(super) fn map_for_reapi(
    kind: &CCompilerKind,
    parsed_args: &ParsedArguments,
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
    if !matches!(parsed_args.language, Language::C | Language::Cxx) {
        return Err(NotEligible::new(
            CompileLocallyReason::UnsupportedLanguage,
            anyhow!("{}", parsed_args.language.as_str()),
        ));
    }
    path_mapper(cwd, env_vars)?;
    // Workers can't run gcc or clang without their toolchain in the input root.
    Err(CompileLocallyReason::CToolchainUnsupported.into())
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

#[cfg(all(test, unix))]
mod test {
    use super::*;
    use crate::compiler::CompilerArguments;
    use crate::compiler::{clang, gcc};
    use test_case::test_case;

    const WS: &str = "/ws/app";
    const OUT_DIR: &str = "/ws/target/debug/build/foo-1/out";
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
    ) -> Result<ReapiCompile, NotEligible> {
        let parsed_args = parse(kind, Path::new(cwd), args);
        map_for_reapi(kind, &parsed_args, Path::new(cwd), &env_vars(env))
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

    #[test_case(CCompilerKind::Diab, &[], CompileLocallyReason::UnsupportedCompiler ;
        "other compiler")]
    #[test_case(CCompilerKind::Gcc, &["-x", "assembler"], CompileLocallyReason::UnsupportedLanguage ;
        "assembler")]
    fn test_compiles_that_stay_local(
        kind: CCompilerKind,
        args: &[&str],
        expected: CompileLocallyReason,
    ) {
        assert_eq!(expected, reason(map(&kind, args)));
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

    #[test]
    fn test_mapped_compiles_wait_for_the_c_toolchain() {
        let mapped = map(&CCompilerKind::Gcc, &[]);
        assert_eq!(CompileLocallyReason::CToolchainUnsupported, reason(mapped));
    }
}
