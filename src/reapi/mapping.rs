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

//! Local paths mapped into a REAPI input root, so actions don't depend on where the checkout
//! lives.
//!
//! Paths under a compile's cwd keep their relative path in the input root, paths under its
//! build root (cargo's target dir) go under [`BUILD_ROOT_DIR`], and the toolchain goes
//! under [`TOOLCHAIN_DIR`].

use crate::errors::*;
use std::path::{Component, Path, PathBuf};

/// The input root's directory for files under the local build root.
pub(super) const BUILD_ROOT_DIR: &str = ".sccache-build";
/// The input root's directory for the toolchain's files.
pub(super) const TOOLCHAIN_DIR: &str = ".sccache-toolchain";

/// A compile that can run through REAPI, with its paths mapped into the input root.
#[derive(Debug)]
pub struct ReapiCompile {
    pub(crate) path_mapper: PathMapper,
    pub(crate) arguments: Vec<String>,
    /// The crate root when cargo passes it as an absolute path, which `arguments` leave out.
    /// A worker passes it as an absolute path too, since rustc writes the crate root into its
    /// output as it was given.
    pub(crate) mapped_crate_root: Option<String>,
    pub(crate) target_triple: String,
    pub(crate) env: ActionEnv,
    /// Local paths of the files that go into the input root.
    pub(crate) input_root_files: Vec<PathBuf>,
    /// Local paths the compile writes.
    pub(crate) outputs: Vec<PathBuf>,
}

/// The environment a compile's action runs with.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct ActionEnv {
    /// Variables set to these values.
    pub(crate) vars: Vec<(String, String)>,
    /// Variables holding paths, given relative to the input root. The command sets each to the
    /// worker's absolute path for it, so `env!("CARGO_MANIFEST_DIR")` names a directory that
    /// exists on the worker.
    pub(crate) path_vars: Vec<(String, String)>,
    /// Variables the crate reads that aren't set locally, which the command unsets.
    pub(crate) unset_vars: Vec<String>,
}

/// Error for a compile that can't run through REAPI.
#[derive(Debug)]
pub struct NotEligible {
    pub(crate) reason: CompileLocallyReason,
    /// The error behind `reason`, if any.
    pub(crate) detail: Option<Error>,
}

impl NotEligible {
    pub(crate) fn new(reason: CompileLocallyReason, detail: Error) -> Self {
        NotEligible {
            reason,
            detail: Some(detail),
        }
    }
}

impl From<CompileLocallyReason> for NotEligible {
    fn from(reason: CompileLocallyReason) -> Self {
        NotEligible {
            reason,
            detail: None,
        }
    }
}

/// Why a compile runs locally instead of through REAPI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompileLocallyReason {
    NotRust,
    RustcDriver,
    WindowsPaths,
    Staticlib,
    UnexpectedCargoLayout,
    PathOutsideInputRoot,
    SourceOutsideInputRoot,
    AbsolutePathInOption,
    CustomTargetSpec,
    NativeCpu,
    ProfileUse,
    RemapPathPrefix,
    NonUtf8EnvVar,
    UnsupportedEnvVarName,
    SecretLikeEnvVar,
    UnreadableDependencies,
}

impl std::fmt::Display for CompileLocallyReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::NotRust => "not a Rust compile",
            Self::RustcDriver => "not rustc",
            Self::WindowsPaths => "Windows paths",
            Self::Staticlib => "staticlib crate type",
            Self::UnexpectedCargoLayout => "unexpected cargo dir layout",
            Self::PathOutsideInputRoot => "path outside the cwd and build root",
            Self::SourceOutsideInputRoot => "source file outside the cwd and build root",
            Self::AbsolutePathInOption => "absolute path in a -C or -Z option",
            Self::CustomTargetSpec => "custom target spec",
            Self::NativeCpu => "-C target-cpu=native or -Z tune-cpu=native",
            Self::ProfileUse => "-C profile-use or -C profile-sample-use",
            Self::RemapPathPrefix => "--remap-path-prefix",
            Self::NonUtf8EnvVar => "non-UTF-8 env var",
            Self::UnsupportedEnvVarName => "env var the REAPI command can't pass to rustc",
            Self::SecretLikeEnvVar => "secret-like env var",
            Self::UnreadableDependencies => "unreadable dependencies",
        })
    }
}

/// Maps local paths under the cwd and build root to input-root-relative paths.
#[derive(Debug)]
pub(crate) struct PathMapper {
    cwd: PathBuf,
    build_root: PathBuf,
}

impl PathMapper {
    pub(crate) fn new(cwd: PathBuf, build_root: PathBuf) -> Self {
        PathMapper { cwd, build_root }
    }

    /// The input-root-relative path for `local`, or `None` if it lies outside both the
    /// build root and the cwd, or isn't UTF-8.
    pub(crate) fn to_input_root(&self, local: &Path) -> Option<String> {
        let absolute = lexically_normalize(&self.cwd.join(local));
        // Checks the build root first, since cargo's target dir is usually inside the
        // workspace root, rustc's cwd for workspace members.
        if let Ok(relative) = absolute.strip_prefix(&self.build_root) {
            let relative = relative.to_str()?;
            return Some(if relative.is_empty() {
                BUILD_ROOT_DIR.to_owned()
            } else {
                format!("{BUILD_ROOT_DIR}/{relative}")
            });
        }
        let relative = absolute.strip_prefix(&self.cwd).ok()?.to_str()?;
        // It would land in the build root's or toolchain's directory in the input root.
        if Path::new(relative).starts_with(BUILD_ROOT_DIR)
            || Path::new(relative).starts_with(TOOLCHAIN_DIR)
        {
            return None;
        }
        Some(if relative.is_empty() {
            ".".to_owned()
        } else {
            relative.to_owned()
        })
    }
}

/// Resolves `.` and `..` in `path` without touching the filesystem.
pub(crate) fn lexically_normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod test {
    use super::*;

    fn path_mapper() -> PathMapper {
        PathMapper::new("/ws".into(), "/ws/target".into())
    }

    #[cfg(unix)]
    #[test]
    fn test_maps_cwd_and_build_root() {
        let mapper = path_mapper();
        assert_eq!(
            "src/lib.rs",
            mapper.to_input_root(Path::new("src/lib.rs")).unwrap()
        );
        assert_eq!(".", mapper.to_input_root(Path::new("/ws")).unwrap());
        assert_eq!(
            ".sccache-build/debug/deps/libfoo.rmeta",
            mapper
                .to_input_root(Path::new("/ws/target/debug/deps/libfoo.rmeta"))
                .unwrap()
        );
        assert_eq!(
            ".sccache-build/debug/deps",
            mapper
                .to_input_root(Path::new("target/debug/deps"))
                .unwrap()
        );
        assert_eq!(
            "src/lib.rs",
            mapper
                .to_input_root(Path::new("/ws/target/../src/lib.rs"))
                .unwrap()
        );
        assert_eq!(None, mapper.to_input_root(Path::new("/elsewhere/x.rs")));
        assert_eq!(None, mapper.to_input_root(Path::new("../x.rs")));
        assert_eq!(None, mapper.to_input_root(Path::new(".sccache-build/x.rs")));
        assert_eq!(
            None,
            mapper.to_input_root(Path::new(".sccache-toolchain/x.rs"))
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_non_utf8_paths_dont_map() {
        use std::os::unix::ffi::OsStrExt;
        let mapper = path_mapper();
        let name = std::ffi::OsStr::from_bytes(b"\xff.rs");
        assert_eq!(None, mapper.to_input_root(&Path::new("/ws/src").join(name)));
        assert_eq!(
            None,
            mapper.to_input_root(&Path::new("/ws/target").join(name))
        );
    }
}
