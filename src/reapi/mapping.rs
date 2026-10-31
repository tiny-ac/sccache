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
//! A worker is a machine that runs actions. It runs each one in a directory, the worker
//! root, that holds the input root.
//!
//! Paths under a compile's cwd keep their relative path in the input root, paths under its
//! build root (cargo's target dir) go under [`BUILD_ROOT_DIR`], and the toolchain goes
//! under [`TOOLCHAIN_DIR`].

use super::client::{CPP_COMPILE_MNEMONIC, RUSTC_MNEMONIC};
use crate::compiler::CCompilerKind;
use crate::errors::*;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};
use std::time::SystemTime;
use tempfile::TempPath;

/// The input root's directory for files under the local build root.
pub(super) const BUILD_ROOT_DIR: &str = ".sccache-build";
/// The input root's directory for the toolchain's files.
pub(super) const TOOLCHAIN_DIR: &str = ".sccache-toolchain";

/// A compile that can run through REAPI, with its paths mapped into the input root.
#[derive(Debug, Serialize, Deserialize)]
pub struct ReapiCompile {
    pub(crate) path_mapper: PathMapper,
    pub(crate) arguments: Vec<String>,
    pub(crate) program: WorkerProgram,
    /// The toolchain whose files go into the input root.
    pub(crate) toolchain: ToolchainKey,
    pub(crate) env: ActionEnv,
    /// Local paths of the files that go into the input root.
    pub(crate) input_root_files: Vec<PathBuf>,
    pub(crate) preprocessed_source: Option<PreprocessedSource>,
    /// Local paths the compile writes.
    pub(crate) outputs: Vec<PathBuf>,
}

/// A C or C++ source preprocessed for a worker, which the input root holds at the source's
/// own path.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PreprocessedSource {
    /// The local file.
    pub(crate) path: PathBuf,
    /// The source's path in the input root.
    pub(crate) mapped_path: String,
    /// Removes the file when the CLI's compile ends. The daemon's copy of the compile has
    /// `None` here, so it leaves the file.
    #[serde(skip)]
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "C misses don't compile as a worker yet")
    )]
    pub(super) temp_path: Option<TempPath>,
}

impl PreprocessedSource {
    pub(crate) fn new(temp_path: TempPath, mapped_path: String) -> Self {
        PreprocessedSource {
            path: temp_path.to_path_buf(),
            mapped_path,
            temp_path: Some(temp_path),
        }
    }
}

/// The program a worker runs for a compile, with what its command line needs besides
/// `ReapiCompile::arguments`.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum WorkerProgram {
    Rustc {
        /// The crate root when cargo passes it as an absolute path, which `arguments` leave
        /// out. A worker passes it as an absolute path too, since rustc writes the crate root
        /// into its output as it was given.
        mapped_crate_root: Option<String>,
    },
    /// gcc or clang, run from the driver that the compile's `ToolchainKey::C` names, which
    /// the input root holds under [`TOOLCHAIN_DIR`].
    C,
}

impl WorkerProgram {
    pub(super) fn action_mnemonic(&self) -> &'static str {
        match self {
            WorkerProgram::Rustc { .. } => RUSTC_MNEMONIC,
            WorkerProgram::C => CPP_COMPILE_MNEMONIC,
        }
    }
}

/// A local toolchain.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum ToolchainKey {
    Rustc(RustcToolchainKey),
    C(CToolchainKey),
}

impl ToolchainKey {
    /// Reads the stamps, so call it from a blocking thread.
    pub(crate) fn rustc(commit_hash: String, sysroot: PathBuf, target_triple: String) -> Self {
        ToolchainKey::Rustc(RustcToolchainKey::new(commit_hash, sysroot, target_triple))
    }

    #[cfg(test)]
    pub(crate) fn as_rustc(&self) -> &RustcToolchainKey {
        match self {
            ToolchainKey::Rustc(key) => key,
            ToolchainKey::C(key) => panic!("{key:?}"),
        }
    }
}

impl std::fmt::Display for ToolchainKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ToolchainKey::Rustc(key) => write!(f, "the toolchain in {}", key.sysroot.display()),
            ToolchainKey::C(key) => write!(f, "the toolchain of {}", key.driver.display()),
        }
    }
}

/// A local Rust toolchain. Two rustcs can report the same commit and still have different
/// files, so the key includes the sysroot, and the stamps of rustc, its libraries' directory
/// and the target's std directory, which change when the toolchain is updated in place or
/// gets a target added.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct RustcToolchainKey {
    pub(crate) commit_hash: String,
    pub(crate) sysroot: PathBuf,
    pub(crate) target_triple: String,
    stamps: Vec<Option<Stamp>>,
}

impl RustcToolchainKey {
    /// Reads the stamps, so call it from a blocking thread.
    pub(crate) fn new(commit_hash: String, sysroot: PathBuf, target_triple: String) -> Self {
        let stamps = [
            sysroot.join("bin/rustc"),
            sysroot.join("lib"),
            sysroot.join("lib/rustlib").join(&target_triple).join("lib"),
        ]
        .iter()
        .map(|path| stamp(path))
        .collect();
        RustcToolchainKey {
            commit_hash,
            sysroot,
            target_triple,
            stamps,
        }
    }
}

/// A local gcc or clang: its driver's canonical path, the files besides the driver that it
/// runs or reads to compile, and the shared libraries they load other than glibc's. The key
/// includes their stamps, which change when one of them is updated in place.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct CToolchainKey {
    pub(crate) kind: CCompilerKind,
    pub(crate) driver: PathBuf,
    pub(crate) internal_files: Vec<InternalFile>,
    pub(crate) libraries: Vec<PathBuf>,
    stamps: Vec<Option<Stamp>>,
}

impl CToolchainKey {
    /// Lists the libraries and reads the stamps, so call it from a blocking thread.
    pub(crate) fn new(
        kind: CCompilerKind,
        driver: PathBuf,
        internal_files: Vec<InternalFile>,
    ) -> Result<Self> {
        let programs = std::iter::once(&driver).chain(
            (internal_files.iter())
                .filter(|file| file.is_program())
                .map(|file| &file.path),
        );
        let mut libraries = std::collections::BTreeSet::new();
        for program in programs {
            libraries.extend(shared_libraries(program)?);
        }
        Ok(Self::with_libraries(
            kind,
            driver,
            internal_files,
            libraries.into_iter().collect(),
        ))
    }

    /// Reads the stamps, so call it from a blocking thread.
    pub(crate) fn with_libraries(
        kind: CCompilerKind,
        driver: PathBuf,
        internal_files: Vec<InternalFile>,
        libraries: Vec<PathBuf>,
    ) -> Self {
        let stamps = std::iter::once(&driver)
            .chain(internal_files.iter().map(|file| &file.path))
            .chain(&libraries)
            .map(|path| stamp(path))
            .collect();
        CToolchainKey {
            kind,
            driver,
            internal_files,
            libraries,
            stamps,
        }
    }
}

/// A file that gcc runs or reads to compile.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct InternalFile {
    /// The driver option that prints its path, such as `-print-prog-name=cc1`.
    pub(crate) option: String,
    pub(crate) path: PathBuf,
}

impl InternalFile {
    /// Whether the driver runs the file, rather than reading it.
    pub(crate) fn is_program(&self) -> bool {
        self.option.starts_with("-print-prog-name=")
    }
}

/// Shared libraries of glibc, which must come from the worker: each works only with the loader
/// and the other libraries of its own glibc.
#[cfg(target_os = "linux")]
pub(super) const GLIBC_LIBRARIES: &[&str] = &[
    "libc.so",
    "libm.so",
    "ld-linux",
    "libpthread.so",
    "libdl.so",
    "librt.so",
];

/// The shared libraries that `program` loads, other than glibc's. A script loads none.
#[cfg(target_os = "linux")]
fn shared_libraries(program: &Path) -> Result<Vec<PathBuf>> {
    let mut magic = [0; 4];
    let is_elf = std::fs::File::open(program)
        .and_then(|mut file| std::io::Read::read_exact(&mut file, &mut magic))
        .is_ok_and(|()| magic == *b"\x7fELF");
    if !is_elf {
        return Ok(vec![]);
    }
    let libraries = crate::dist::ldd::find_ldd_libraries(program)
        .with_context(|| format!("failed to list the libraries of {}", program.display()))?;
    Ok(libraries
        .into_iter()
        .filter(|library| {
            let name = library.file_name().unwrap_or_default().to_string_lossy();
            !GLIBC_LIBRARIES.iter().any(|glibc| name.starts_with(glibc))
        })
        .collect())
}

/// Workers run Linux, so another OS's compiler never runs there.
#[cfg(not(target_os = "linux"))]
fn shared_libraries(_program: &Path) -> Result<Vec<PathBuf>> {
    Ok(vec![])
}

/// A file's size and modification time.
type Stamp = (u64, SystemTime);

fn stamp(path: &Path) -> Option<Stamp> {
    let metadata = std::fs::metadata(path).ok()?;
    Some((metadata.len(), metadata.modified().ok()?))
}

/// The environment a compile's action runs with.
#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompileLocallyReason {
    UnsupportedCompiler,
    RustcDriver,
    NoCommitHash,
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
    MappingPanicked,
    Misconfigured,
    Connecting,
    ConnectFailed,
    IncompatibleServer,
    CredentialsRefused,
    CredentialHelperFailed,
    HashingToolchain,
    HashingToolchainFailed,
    MissingRustcOrStd,
    NonUtf8ToolchainPath,
    ColonOrEqualsInPath,
    CacheBypassed,
    LocalCompileUnchanged,
    HashingInputsFailed,
    NotInActionCache,
    ActionCacheLookupFailed,
    ActionCacheLookupsPaused,
    UnusableResult,
    WorkerRootInOutput,
    DownloadingOutputsFailed,
    UnwritableOutputs,
    ExecutionUnsupported,
    NonLinuxToolchain,
    UnpackingWorkerToolchain,
    UnusableWorkerToolchain,
    ProcMacroForClientOs,
    WorkerStdDiffers,
    RustcFailsOnWorkers,
    RustcCommitDiffersOnWorkers,
    ExecutionsPaused,
    LocalSlotFree,
    NoFreeRemoteSlot,
    ExecutionsRefused,
    UnreadableInput,
    UploadFailed,
    ExecutionFailed,
    ExecutionQueuedTooLong,
    ExecutionTimedOut,
    CompileFailedOnWorker,
    DaemonFailed,
    UnsupportedLanguage,
    LanguageAfterInput,
    NotFromBuildScript,
    CompilerEnvVar,
    ArchOption,
    PrecompiledHeader,
    ProfilingOrCoverage,
    MacroDebugInfo,
    PrefixMapOption,
    OptionReadsFile,
    CompilerInternalsOption,
    VerboseOutput,
    RecordedCommandLine,
    ExtraOutput,
    NonUtf8Argument,
    CToolchainUnsupported,
    PreprocessingFailed,
    PreprocessedSourceNotWritten,
    MissingGccProgram,
    SharedLibrariesNotListed,
    ClangConfigFile,
    SideOutputOption,
}

impl std::fmt::Display for CompileLocallyReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::UnsupportedCompiler => "unsupported compiler",
            Self::RustcDriver => "not rustc",
            Self::NoCommitHash => "rustc without a commit hash",
            Self::WindowsPaths => "Windows paths",
            Self::Staticlib => "staticlib crate type",
            Self::UnexpectedCargoLayout => "unexpected cargo dir layout",
            Self::PathOutsideInputRoot => "path outside the cwd and build root",
            Self::SourceOutsideInputRoot => "source file outside the cwd and build root",
            Self::AbsolutePathInOption => "absolute path in an option",
            Self::CustomTargetSpec => "custom target spec",
            Self::NativeCpu => "native CPU option",
            Self::ProfileUse => "-C profile-use or -C profile-sample-use",
            Self::RemapPathPrefix => "--remap-path-prefix",
            Self::NonUtf8EnvVar => "non-UTF-8 env var",
            Self::UnsupportedEnvVarName => "env var the REAPI command can't pass to rustc",
            Self::SecretLikeEnvVar => "secret-like env var",
            Self::UnreadableDependencies => "unreadable dependencies",
            Self::MappingPanicked => "mapping the compile for REAPI panicked",
            Self::Misconfigured => "[dist.reapi] misconfigured",
            Self::Connecting => "connecting to the REAPI server",
            Self::ConnectFailed => "failed to connect to the REAPI server",
            Self::IncompatibleServer => "incompatible REAPI server",
            Self::CredentialsRefused => "REAPI server refused the credentials",
            Self::CredentialHelperFailed => "REAPI credential helper failed",
            Self::HashingToolchain => "hashing the toolchain",
            Self::HashingToolchainFailed => "failed to hash the toolchain",
            Self::MissingRustcOrStd => "toolchain without rustc or std",
            Self::NonUtf8ToolchainPath => "non-UTF-8 toolchain path",
            Self::ColonOrEqualsInPath => "`:` or `=` in a toolchain dir or the worker root",
            Self::CacheBypassed => "SCCACHE_RECACHE or SCCACHE_NO_CACHE set",
            Self::LocalCompileUnchanged => "local compile without the worker's arguments",
            Self::HashingInputsFailed => "failed to hash the inputs",
            Self::NotInActionCache => "not in the action cache",
            Self::ActionCacheLookupFailed => "action cache lookup failed",
            Self::ActionCacheLookupsPaused => "action cache lookups paused after failures",
            Self::UnusableResult => "unusable action cache result",
            Self::WorkerRootInOutput => "worker root in an output",
            Self::DownloadingOutputsFailed => "failed to download the outputs",
            Self::UnwritableOutputs => "failed to write the outputs",
            Self::ExecutionUnsupported => "REAPI server doesn't execute actions",
            Self::NonLinuxToolchain => "toolchain that doesn't run on Linux workers",
            Self::UnpackingWorkerToolchain => "unpacking the worker toolchain",
            Self::UnusableWorkerToolchain => "unusable worker toolchain archive",
            Self::ProcMacroForClientOs => "proc macro for the client's OS",
            Self::WorkerStdDiffers => "worker toolchain's std differs from the local one",
            Self::RustcFailsOnWorkers => "rustc -vV fails on the workers",
            Self::RustcCommitDiffersOnWorkers => "rustc on the workers reports another commit hash",
            Self::ExecutionsPaused => "REAPI executions paused after failures",
            Self::LocalSlotFree => "a local slot is free",
            Self::NoFreeRemoteSlot => "no remote slot freed in time",
            Self::ExecutionsRefused => "REAPI server refused executions",
            Self::UnreadableInput => "failed to read an input to upload it",
            Self::UploadFailed => "failed to upload the action's blobs",
            Self::ExecutionFailed => "REAPI execution failed",
            Self::ExecutionQueuedTooLong => "REAPI execution queued too long",
            Self::ExecutionTimedOut => "REAPI action ran out of time on a worker",
            Self::CompileFailedOnWorker => "compile failed on a worker",
            Self::DaemonFailed => "the sccache daemon failed",
            Self::UnsupportedLanguage => "language other than C or C++",
            Self::LanguageAfterInput => "-x after the input",
            Self::NotFromBuildScript => "not run by a cargo build script",
            Self::CompilerEnvVar => "env var that changes what the compiler runs",
            Self::ArchOption => "-arch option",
            Self::PrecompiledHeader => "precompiled header",
            Self::ProfilingOrCoverage => "profiling or coverage option",
            Self::MacroDebugInfo => "debug info with macros",
            Self::PrefixMapOption => "the compile's own -f*-prefix-map",
            Self::OptionReadsFile => "option that reads a file besides the source",
            Self::CompilerInternalsOption => "option passed to the compiler's internal programs",
            Self::VerboseOutput => "verbose compiler output",
            Self::RecordedCommandLine => "option that records the command line",
            Self::ExtraOutput => "output besides the object and the depfile",
            Self::NonUtf8Argument => "non-UTF-8 argument",
            Self::CToolchainUnsupported => "C toolchain unsupported on workers",
            Self::PreprocessingFailed => "preprocessing for REAPI failed",
            Self::PreprocessedSourceNotWritten => "failed to write the preprocessed source",
            Self::MissingGccProgram => "gcc without its cc1, cc1plus or as",
            Self::SharedLibrariesNotListed => "failed to list the compiler's shared libraries",
            Self::ClangConfigFile => "clang config file",
            Self::SideOutputOption => "option that writes a file besides the object",
        })
    }
}

/// Maps local paths under the cwd and build root to input-root-relative paths.
#[derive(Clone, Debug, Serialize, Deserialize)]
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

    /// The inverse of [`Self::to_input_root`].
    pub(super) fn to_local(&self, input_root_path: &str) -> PathBuf {
        match Path::new(input_root_path).strip_prefix(BUILD_ROOT_DIR) {
            Ok(path_in_build_root) => self.build_root.join(path_in_build_root),
            Err(_) => self.cwd.join(input_root_path),
        }
    }

    /// The cwd and the build root, each with the path a worker's compile names it by. The
    /// build root, often inside the cwd, comes last.
    pub(crate) fn input_root_prefixes(&self) -> [(&Path, &str); 2] {
        [(&self.cwd, "."), (&self.build_root, BUILD_ROOT_DIR)]
    }

    /// `<option><local path>=<path in the input root>` arguments for
    /// [`Self::input_root_prefixes`], in their order, for compilers that apply the last
    /// matching mapping.
    pub(crate) fn prefix_map_args(&self, option: &str) -> Vec<OsString> {
        self.input_root_prefixes()
            .into_iter()
            .map(|(from, to)| {
                let mut arg = OsString::from(option);
                arg.push(from);
                arg.push("=");
                arg.push(to);
                arg
            })
            .collect()
    }

    /// `--remap-path-prefix` arguments that make a local compile name its paths as a
    /// worker's compile does.
    pub(crate) fn input_root_remap_args(&self) -> Vec<OsString> {
        self.prefix_map_args("--remap-path-prefix=")
    }

    /// Maps paths under `worker_root`, and input-root-relative paths under
    /// [`BUILD_ROOT_DIR`], in an action's stdout or stderr to local paths.
    pub(super) fn worker_paths_to_local(&self, text: &[u8], worker_root: &str) -> Vec<u8> {
        self.paths_to_local(text, Some(worker_root.as_bytes()))
    }

    /// Maps [`BUILD_ROOT_DIR`] paths in the stdout or stderr of a local compile run with
    /// [`Self::input_root_remap_args`] to local paths.
    pub(crate) fn input_root_paths_to_local(&self, text: &[u8]) -> Vec<u8> {
        self.paths_to_local(text, None)
    }

    /// Maps paths in `text` to local paths, rewriting only each path's start, so where a path
    /// ends doesn't matter. With `worker_root`, paths under it map too.
    fn paths_to_local(&self, text: &[u8], worker_root: Option<&[u8]>) -> Vec<u8> {
        let cwd = self.cwd.as_os_str().as_encoded_bytes();
        let build_root = self.build_root.as_os_str().as_encoded_bytes();
        let mut out = Vec::with_capacity(text.len());
        let mut i = 0;
        while i < text.len() {
            if !starts_path(&text[..i]) {
                out.push(text[i]);
                i += 1;
                continue;
            }
            let (in_worker_root, path) = match worker_root.and_then(|r| text[i..].strip_prefix(r)) {
                Some([b'/', rest @ ..]) => (true, rest),
                Some(rest) if rest.first().is_none_or(|&b| !is_path_byte(b)) => (true, rest),
                Some(_) | None => (false, &text[i..]),
            };
            let skipped = text.len() - i - path.len();
            if let Some(rest) = path.strip_prefix(BUILD_ROOT_DIR.as_bytes())
                && rest.first().is_none_or(|&b| b == b'/' || !is_path_byte(b))
            {
                out.extend_from_slice(build_root);
                i += skipped + BUILD_ROOT_DIR.len();
            } else if in_worker_root {
                out.extend_from_slice(cwd);
                // Skipping the worker root's `/` too means a path follows.
                if worker_root.is_some_and(|r| skipped > r.len()) {
                    out.push(b'/');
                }
                i += skipped;
            } else {
                out.push(text[i]);
                i += 1;
            }
        }
        out
    }
}

fn is_path_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'/' | b'.' | b'_' | b'-')
}

/// Whether a path can start right after `before`: at the start, after a byte that can't be
/// in a path, or after a terminal color code, which `--color=always` puts before paths.
fn starts_path(before: &[u8]) -> bool {
    match before.last() {
        None => true,
        Some(b'm') => {
            let params = before[..before.len() - 1]
                .iter()
                .rev()
                .take_while(|b| b.is_ascii_digit() || **b == b';')
                .count();
            before[..before.len() - 1 - params].ends_with(b"\x1b[")
        }
        Some(&b) => !is_path_byte(b),
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
    use test_case::test_case;

    #[test]
    fn test_a_toolchain_key_changes_with_rustc_and_a_new_target() {
        let dir = tempfile::tempdir().unwrap();
        let sysroot = dir.path().to_owned();
        std::fs::create_dir_all(sysroot.join("bin")).unwrap();
        std::fs::write(sysroot.join("bin/rustc"), b"rustc 1").unwrap();
        let key = || {
            ToolchainKey::rustc(
                "abc123".to_owned(),
                sysroot.clone(),
                "wasm32-unknown-unknown".to_owned(),
            )
        };
        let original = key();
        assert_eq!(original, key());

        std::fs::write(sysroot.join("bin/rustc"), b"rustc 1.1").unwrap();
        let updated = key();
        assert_ne!(original, updated, "rustc changed");

        std::fs::create_dir_all(sysroot.join("lib/rustlib/wasm32-unknown-unknown/lib")).unwrap();
        assert_ne!(updated, key(), "target added");
    }

    #[test]
    fn test_a_c_toolchain_key_changes_with_a_library() {
        let dir = tempfile::tempdir().unwrap();
        let (driver, library) = (dir.path().join("gcc"), dir.path().join("libgmp.so.10"));
        std::fs::write(&driver, b"gcc").unwrap();
        std::fs::write(&library, b"gmp 1").unwrap();
        let key = || {
            CToolchainKey::with_libraries(
                CCompilerKind::Gcc,
                driver.clone(),
                vec![],
                vec![library.clone()],
            )
        };
        let original = key();
        assert_eq!(original, key());

        std::fs::write(&library, b"gmp 1.1").unwrap();
        assert_ne!(original, key(), "the library changed");
    }

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

    #[test]
    fn test_maps_input_root_paths_to_local() {
        let mapper = path_mapper();
        assert_eq!(
            PathBuf::from("/ws/target/debug/deps/libfoo.rlib"),
            mapper.to_local(".sccache-build/debug/deps/libfoo.rlib")
        );
        assert_eq!(
            PathBuf::from("/ws/src/lib.rs"),
            mapper.to_local("src/lib.rs")
        );
    }

    #[cfg(unix)]
    #[test_case(
        "warning: in /up/src/lib.rs\n --> .sccache-build/debug/build/foo-2/out/gen.rs:3:5\n",
        "warning: in /ws/src/lib.rs\n --> /ws/target/debug/build/foo-2/out/gen.rs:3:5\n" ;
        "worker root and build root paths")]
    #[test_case(
        "/up/.sccache-build/debug/deps/libbar.rlib",
        "/ws/target/debug/deps/libbar.rlib" ;
        "build root under the worker root")]
    #[test_case("cd /up: `/up` in /up", "cd /ws: `/ws` in /ws" ; "the worker root itself")]
    #[test_case("\x1b[0m\x1b[1m/up/x", "\x1b[0m\x1b[1m/ws/x" ; "after a color code")]
    #[test_case(
        "/home/up/x /upper/x foo.sccache-build/x",
        "/home/up/x /upper/x foo.sccache-build/x" ;
        "only at the start of a path")]
    fn test_worker_paths_map_to_local(worker_text: &str, local_text: &str) {
        assert_eq!(
            local_text,
            String::from_utf8(path_mapper().worker_paths_to_local(worker_text.as_bytes(), "/up"))
                .unwrap()
        );
    }

    #[test]
    fn test_input_root_remap_puts_the_build_root_last() {
        // `path_mapper()`'s build root is inside its cwd.
        assert_eq!(
            [
                "--remap-path-prefix=/ws=.",
                "--remap-path-prefix=/ws/target=.sccache-build"
            ],
            path_mapper().input_root_remap_args().as_slice()
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_input_root_paths_map_to_local() {
        assert_eq!(
            b"--> /ws/target/debug/build/foo-1/out/gen.rs:3:5 in /up/x".as_slice(),
            path_mapper().input_root_paths_to_local(
                b"--> .sccache-build/debug/build/foo-1/out/gen.rs:3:5 in /up/x"
            )
        );
    }

    #[test]
    fn test_mapping_worker_paths_keeps_other_bytes() {
        assert_eq!(
            b"\xff /ws/x\xfe".as_slice(),
            path_mapper().worker_paths_to_local(b"\xff /up/x\xfe", "/up")
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

    /// A sysroot in `dir` of symlinks to the real one's entries, with a rust-src that has the
    /// file rustc looks for, so the test sees a rust-src whether or not one is installed.
    #[cfg(unix)]
    fn sysroot_with_rust_src(dir: &Path) -> PathBuf {
        let output = std::process::Command::new(crate::test::utils::find_rustc())
            .args(["--print", "sysroot"])
            .output()
            .unwrap();
        let real = PathBuf::from(String::from_utf8(output.stdout).unwrap().trim());
        let sysroot = dir.join("sysroot");
        for (from, skip) in [("", "lib"), ("lib", "rustlib"), ("lib/rustlib", "src")] {
            std::fs::create_dir_all(sysroot.join(from)).unwrap();
            for entry in std::fs::read_dir(real.join(from)).unwrap() {
                let entry = entry.unwrap();
                if entry.file_name() != skip {
                    std::os::unix::fs::symlink(
                        entry.path(),
                        sysroot.join(from).join(entry.file_name()),
                    )
                    .unwrap();
                }
            }
        }
        let std_root = sysroot.join("lib/rustlib/src/rust/library/std/src/lib.rs");
        std::fs::create_dir_all(std_root.parent().unwrap()).unwrap();
        std::fs::write(std_root, "").unwrap();
        sysroot
    }

    /// Compiles a crate at `root` with `sysroot` and the remap arguments a local compile gets
    /// for REAPI, and returns the rlib.
    #[cfg(unix)]
    fn compile_remapped(root: &Path, sysroot: &Path) -> Vec<u8> {
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::create_dir_all(root.join("target/debug/deps")).unwrap();
        // rustc sees its working directory with symlinks resolved, e.g. macOS's /var as
        // /private/var, and so does cargo.
        let root = &root.canonicalize().unwrap();
        std::fs::write(
            root.join("src/lib.rs"),
            // std's generics, built into the rlib with debuginfo, name std's sources.
            "pub fn source_path() -> &'static str { file!() }\n\
             pub fn strings() -> Vec<String> { vec![format!(\"{}\", 1)] }\n",
        )
        .unwrap();
        let path_mapper = PathMapper::new(root.to_owned(), root.join("target"));
        let mut sysroot_arg = OsString::from("--sysroot=");
        sysroot_arg.push(sysroot);
        let status = std::process::Command::new(crate::test::utils::find_rustc())
            .current_dir(root)
            .args([
                "--crate-name=foo",
                "--crate-type=lib",
                "--edition=2021",
                "-Cdebuginfo=2",
                "-Cmetadata=fixed",
                "--emit=link",
                "-o",
                "target/debug/deps/libfoo.rlib",
                "src/lib.rs",
            ])
            .args(path_mapper.input_root_remap_args())
            .arg(sysroot_arg)
            .status()
            .unwrap();
        assert!(status.success());
        std::fs::read(root.join("target/debug/deps/libfoo.rlib")).unwrap()
    }

    #[cfg(unix)]
    fn names_rust_src(rlib: &[u8]) -> bool {
        memchr::memmem::find(rlib, b"rustlib/src/rust").is_some()
    }

    #[cfg(unix)]
    #[test]
    fn test_remapped_compiles_match_across_checkouts_and_sysroot_copies() {
        let dir = tempfile::tempdir().unwrap();
        let sysroot = sysroot_with_rust_src(dir.path());
        // Outside the checkouts, so the sysroot copies' paths aren't remapped.
        let copy_a = crate::reapi::sysroot_copy(&sysroot, &dir.path().join("copies-a")).unwrap();
        let copy_b = crate::reapi::sysroot_copy(&sysroot, &dir.path().join("copies-b")).unwrap();
        let a = compile_remapped(&dir.path().join("a/checkout"), &copy_a);
        let b = compile_remapped(&dir.path().join("b/another-checkout"), &copy_b);
        assert!(!names_rust_src(&a), "the rlib names rust-src");
        assert!(a == b, "the rlibs differ");
    }

    #[cfg(unix)]
    #[test]
    fn test_a_sysroot_with_rust_src_puts_it_in_the_rlib() {
        // Shows that the test above would see rust-src if the copy kept it.
        let dir = tempfile::tempdir().unwrap();
        let sysroot = sysroot_with_rust_src(dir.path());
        let rlib = compile_remapped(&dir.path().join("checkout"), &sysroot);
        assert!(names_rust_src(&rlib), "the rlib doesn't name rust-src");
    }
}
