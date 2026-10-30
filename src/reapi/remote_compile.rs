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

//! A Rust compile's REAPI command and toolchain files, and writing an action result's
//! output files to their local paths.

use super::cas::{Blob, Cas};
use super::digest::{self, DigestCache};
use super::input_root::{InputRoot, InputRootFile};
use super::mapping::{BUILD_ROOT_DIR, CompileLocallyReason, ReapiCompile, TOOLCHAIN_DIR};
use super::proto::build::bazel::remote::execution::v2 as remexec;
use crate::config::ReapiConfig;
use crate::errors::*;
use crate::mock_command::exit_status;
use memchr::memmem;
use prost::Message;
use std::collections::HashSet;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process;
use tempfile::{NamedTempFile, TempPath};

/// Starts the stderr line where the worker reports the worker root.
pub(super) const WORKER_ROOT_MARKER: &str = "sccache-reapi-pwd:";

/// The command a worker runs for `compile`, which doesn't depend on the checkout path. A
/// shell script adds the worker root, known only when the action runs, to path env vars and
/// rustc's arguments.
pub(super) fn build_command(compile: &ReapiCompile) -> remexec::Command {
    // rustc's paths use the physical cwd, so $PWD must too for the remaps and the worker
    // root line to match them. `materialize` reads the worker root line to map the worker
    // root to local paths.
    let mut script = format!(
        "set -e\ncd -P .\nPWD=$(pwd -P)\nprintf '{WORKER_ROOT_MARKER}%s\\n' \"$PWD\" >&2\n"
    );
    // Names go into the script unquoted; the mapping only lets identifiers through.
    debug_assert!(
        compile
            .env
            .path_vars
            .iter()
            .map(|(name, _)| name)
            .chain(&compile.env.unset_vars)
            .all(|name| is_identifier(name)),
        "{:?}",
        compile.env
    );
    let mut unset_vars = compile.env.unset_vars.clone();
    unset_vars.sort();
    if !unset_vars.is_empty() {
        script.push_str(&format!("unset {}\n", unset_vars.join(" ")));
    }
    let mut path_vars = compile.env.path_vars.clone();
    path_vars.sort();
    for (name, path) in &path_vars {
        script.push_str(&format!("export {name}=\"$PWD\"/{}\n", sh_quote(path)));
    }
    let mapped_crate_root_arg = match &compile.mapped_crate_root {
        Some(path) => format!(" \"$PWD\"/{}", sh_quote(path)),
        None => String::new(),
    };
    // Execution may hardlink input root files from their CAS, so rustc may not find its
    // libraries and sysroot from its own path; the script sets both.
    // rustc applies the last matching --remap-path-prefix, so BUILD_ROOT_DIR's comes last.
    script.push_str(&format!(
        "export LD_LIBRARY_PATH=\"$PWD\"/{TOOLCHAIN_DIR}/lib DYLD_LIBRARY_PATH=\"$PWD\"/{TOOLCHAIN_DIR}/lib\n\
         exec \"$PWD\"/{TOOLCHAIN_DIR}/bin/rustc --sysroot \"$PWD\"/{TOOLCHAIN_DIR} \"$@\"{mapped_crate_root_arg} \
         --remap-path-prefix=\"$PWD\"=. --remap-path-prefix=\"$PWD\"/{BUILD_ROOT_DIR}={BUILD_ROOT_DIR}\n"
    ));
    let mut arguments = vec![
        "/bin/sh".to_owned(),
        "-c".to_owned(),
        script,
        "rustc".to_owned(),
    ];
    arguments.extend(compile.arguments.iter().cloned());
    // REAPI requires both lists sorted.
    let mut vars = compile.env.vars.clone();
    vars.sort();
    let mut output_paths: Vec<String> = command_output_paths(compile).collect();
    output_paths.sort();
    output_paths.dedup();
    remexec::Command {
        arguments,
        environment_variables: vars
            .into_iter()
            .map(|(name, value)| remexec::command::EnvironmentVariable { name, value })
            .collect(),
        output_paths,
        ..Default::default()
    }
}

/// The paths `compile`'s command declares as outputs, input-root-relative.
fn command_output_paths(compile: &ReapiCompile) -> impl Iterator<Item = String> + '_ {
    compile
        .outputs
        .iter()
        .filter_map(|output| compile.path_mapper.to_input_root(output))
}

/// `compile`'s own input root files, without its toolchain's. Hashing blocks, so call it from
/// a blocking thread.
pub(super) fn digest_input_root_files(
    compile: &ReapiCompile,
    digest_cache: &DigestCache,
) -> Result<Vec<(InputRootFile, PathBuf)>> {
    compile
        .input_root_files
        .iter()
        .map(|local_path| {
            let path = compile
                .path_mapper
                .to_input_root(local_path)
                .with_context(|| format!("{} is outside the input root", local_path.display()))?;
            let file = InputRootFile {
                path,
                digest: digest_cache.digest(local_path)?,
                is_executable: false,
            };
            Ok((file, local_path.clone()))
        })
        .collect()
}

/// What every action gets from `[dist.reapi]`.
#[derive(Debug, Default, Clone)]
pub(super) struct ActionOptions {
    salt: Vec<u8>,
    platform: Option<remexec::Platform>,
}

impl ActionOptions {
    pub(super) fn new(config: &ReapiConfig) -> Self {
        // BTreeMap order is the sorted order REAPI requires.
        let platform = (!config.platform.is_empty()).then(|| remexec::Platform {
            properties: config
                .platform
                .iter()
                .map(|(name, value)| remexec::platform::Property {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect(),
        });
        ActionOptions {
            salt: config.salt.clone().into_bytes(),
            platform,
        }
    }
}

/// The command that runs `compile`, and the action that runs it on the input root
/// `input_root_digest`.
fn build_action(
    compile: &ReapiCompile,
    input_root_digest: remexec::Digest,
    options: &ActionOptions,
) -> (remexec::Command, remexec::Action) {
    let mut command = build_command(compile);
    // REAPI v2.2+ servers read the platform from the Action; the spec asks to set it on the
    // Command too, for older ones.
    #[expect(deprecated)]
    {
        command.platform = options.platform.clone();
    }
    let action = remexec::Action {
        command_digest: Some(digest::of_bytes(&command.encode_to_vec())),
        input_root_digest: Some(input_root_digest),
        platform: options.platform.clone(),
        salt: options.salt.clone(),
        ..Default::default()
    };
    (command, action)
}

/// The digest of an action, and every blob it needs: its input files, the directories of
/// its input root, its command and itself.
pub(super) struct ActionBlobs {
    pub(super) action_digest: remexec::Digest,
    pub(super) blobs: Vec<(remexec::Digest, Blob)>,
}

/// The [`ActionBlobs`] of the action that runs `compile` on `files` and `toolchain`'s files.
pub(super) fn action_blobs(
    compile: &ReapiCompile,
    files: &[(InputRootFile, PathBuf)],
    toolchain: &[(InputRootFile, PathBuf)],
    options: &ActionOptions,
) -> Result<ActionBlobs> {
    let all_files = files.iter().chain(toolchain);
    let input_root = InputRoot::build(all_files.clone().map(|(file, _)| file))?;
    let (command, action) = build_action(compile, input_root.digest, options);
    let command_bytes = command.encode_to_vec();
    let action_bytes = action.encode_to_vec();
    let action_digest = digest::of_bytes(&action_bytes);
    let mut blobs: Vec<(remexec::Digest, Blob)> = all_files
        .map(|(file, local_path)| (file.digest.clone(), Blob::File(local_path.clone())))
        .collect();
    blobs.extend(
        input_root
            .directories
            .into_iter()
            .map(|(digest, bytes)| (digest, Blob::Bytes(bytes))),
    );
    blobs.push((digest::of_bytes(&command_bytes), Blob::Bytes(command_bytes)));
    blobs.push((action_digest.clone(), Blob::Bytes(action_bytes)));
    Ok(ActionBlobs {
        action_digest,
        blobs,
    })
}

fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn sh_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

/// Error for a toolchain that compiles can't use until it changes.
#[derive(Debug)]
pub(super) struct UnusableToolchain(pub(super) CompileLocallyReason);

impl std::fmt::Display for UnusableToolchain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for UnusableToolchain {}

fn unusable(reason: CompileLocallyReason, detail: String) -> Error {
    Error::new(UnusableToolchain(reason)).context(detail)
}

/// rustc, its shared libraries, and `target_triple`'s std and bundled linker from
/// `sysroot`, as input root files under [`TOOLCHAIN_DIR`] paired with their local paths.
/// Fails with [`UnusableToolchain`] if one is missing or has a non-UTF-8 path.
pub(super) fn toolchain_files(
    sysroot: &Path,
    target_triple: &str,
    digest_cache: &DigestCache,
) -> Result<Vec<(InputRootFile, PathBuf)>> {
    let mut files = vec![];
    let mut add = |local_path: PathBuf, is_executable: bool| -> Result<()> {
        let path_in_sysroot = local_path
            .strip_prefix(sysroot)
            .with_context(|| format!("{} outside {}", local_path.display(), sysroot.display()))?
            .to_str()
            .ok_or_else(|| {
                unusable(
                    CompileLocallyReason::NonUtf8ToolchainPath,
                    format!("non-UTF-8 toolchain path {}", local_path.display()),
                )
            })?
            .to_owned();
        // A symlink becomes a file with its destination's contents, which the CAS stores once.
        let digest = digest_cache.digest_and_pin(&local_path)?;
        let file = InputRootFile {
            path: format!("{TOOLCHAIN_DIR}/{path_in_sysroot}"),
            digest,
            is_executable,
        };
        files.push((file, local_path));
        Ok(())
    };
    let missing = |what: &str| {
        unusable(
            CompileLocallyReason::MissingRustcOrStd,
            format!("no {what} in {}", sysroot.display()),
        )
    };
    let rustc = sysroot.join("bin/rustc");
    if !rustc.is_file() {
        return Err(missing("rustc"));
    }
    add(rustc, true)?;
    // rustc's own shared libraries; the sanitizer runtimes next to them aren't needed to compile.
    let lib_dir = sysroot.join("lib");
    if !lib_dir.is_dir() {
        return Err(missing("lib"));
    }
    let listing_context = || format!("failed to list {}", lib_dir.display());
    for entry in std::fs::read_dir(&lib_dir).with_context(listing_context)? {
        let path = entry.with_context(listing_context)?.path();
        let is_rustc_library = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("librustc_driver") || n.starts_with("libLLVM"));
        if path.is_file() && is_rustc_library {
            add(path, false)?;
        }
    }
    let target_triple_dir = sysroot.join("lib/rustlib").join(target_triple);
    let std_dir = target_triple_dir.join("lib");
    if !std_dir.is_dir() {
        return Err(missing(&format!("std for {target_triple}")));
    }
    for entry in walkdir::WalkDir::new(&std_dir).follow_links(true) {
        let entry = entry?;
        if entry.file_type().is_file() {
            add(entry.into_path(), false)?;
        }
    }
    // rust-lld, the default linker for some target triples, such as x86_64-unknown-linux-gnu.
    let linker_dir = target_triple_dir.join("bin");
    if linker_dir.is_dir() {
        for entry in walkdir::WalkDir::new(&linker_dir).follow_links(true) {
            let entry = entry?;
            if entry.file_type().is_file() {
                add(entry.into_path(), true)?;
            }
        }
    }
    Ok(files)
}

/// Writes to `inner` and records whether the bytes written contain the worker root, even
/// split across writes. The worker root doesn't exist on the client machine, so an output
/// that contains it can't be used there.
struct WorkerRootScanner<W> {
    inner: W,
    finder: memmem::Finder<'static>,
    /// The last `worker_root.len() - 1` bytes written, where a worker root split across
    /// writes would start.
    tail: Vec<u8>,
    contains_worker_root: bool,
}

impl<W: Write> WorkerRootScanner<W> {
    fn new(inner: W, worker_root: &str) -> Self {
        WorkerRootScanner {
            inner,
            finder: memmem::Finder::new(worker_root.as_bytes()).into_owned(),
            tail: vec![],
            contains_worker_root: false,
        }
    }
}

impl<W: Write> Write for WorkerRootScanner<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        let written = &buf[..n];
        if !self.contains_worker_root {
            let tail_len = self.finder.needle().len().saturating_sub(1);
            // A worker root that starts in the tail ends in this write's first `tail_len`
            // bytes.
            self.tail
                .extend_from_slice(&written[..written.len().min(tail_len)]);
            self.contains_worker_root =
                self.finder.find(&self.tail).is_some() || self.finder.find(written).is_some();
            if written.len() >= tail_len {
                self.tail.clear();
                self.tail
                    .extend_from_slice(&written[written.len() - tail_len..]);
            } else {
                self.tail.drain(..self.tail.len().saturating_sub(tail_len));
            }
        }
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// The largest output file downloaded, well above the largest rlibs with debuginfo.
const MAX_OUTPUT_FILE_BYTES: u64 = 4 << 30;
/// The largest stdout or stderr downloaded, which is held in memory.
const MAX_STDOUT_OR_STDERR_BYTES: u64 = 64 << 20;

/// Error for an output file that contains the worker root, such as one from a crate that
/// embeds `env!("CARGO_MANIFEST_DIR")`.
#[derive(Debug)]
pub(super) struct WorkerRootInOutput(PathBuf);

impl std::fmt::Display for WorkerRootInOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} contains the worker root", self.0.display())
    }
}

impl std::error::Error for WorkerRootInOutput {}

/// Error for an action result that lacks a digest or the worker root line, or doesn't match
/// its command or digests.
#[derive(Debug)]
pub(super) struct UnusableResult(String);

impl std::fmt::Display for UnusableResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for UnusableResult {}

/// Error for output files that can't be written to their local paths, such as on a full disk.
#[derive(Debug)]
pub(super) struct UnwritableOutputs;

impl std::fmt::Display for UnwritableOutputs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failed to write the outputs to their local paths")
    }
}

impl std::error::Error for UnwritableOutputs {}

/// An output file's contents, checked for the worker root but not yet at its local path.
enum OutputContents {
    /// Downloaded to a temporary file in its local directory.
    TempFile(TempPath),
    Inline(Vec<u8>),
}

/// Writes `result`'s output files to their local paths once all are downloaded and
/// checked, and returns its stdout and stderr mapped to local paths.
pub(super) async fn materialize(
    cas: &Cas,
    compile: &ReapiCompile,
    result: &remexec::ActionResult,
) -> Result<process::Output> {
    // An exit code that a process can't return, such as 256, would read as success.
    let exit_code = u8::try_from(result.exit_code)
        .map_err(|_| UnusableResult(format!("result has exit code {}", result.exit_code)))?;
    let stdout =
        download_stdout_or_stderr(cas, &result.stdout_raw, result.stdout_digest.as_ref()).await?;
    let stderr =
        download_stdout_or_stderr(cas, &result.stderr_raw, result.stderr_digest.as_ref()).await?;
    let (worker_root, stderr) = take_worker_root(&stderr)?;
    check_output_paths(compile, result)?;
    let output_contents = futures::future::try_join_all(
        result
            .output_files
            .iter()
            .map(|file| download_output(cas, compile, file, &worker_root)),
    )
    .await?;
    let stdout = compile
        .path_mapper
        .worker_paths_to_local(&stdout, &worker_root);
    let stderr = compile
        .path_mapper
        .worker_paths_to_local(&stderr, &worker_root);
    tokio::task::spawn_blocking(move || write_outputs(output_contents))
        .await
        .context(UnwritableOutputs)??;
    Ok(process::Output {
        status: exit_status_from_code(exit_code),
        stdout,
        stderr,
    })
}

/// Why a compile runs locally after `materialize` failed with `e`.
pub(super) fn materialize_failure_reason(e: &Error) -> CompileLocallyReason {
    if e.is::<WorkerRootInOutput>() {
        CompileLocallyReason::WorkerRootInOutput
    } else if e.is::<UnusableResult>() {
        CompileLocallyReason::UnusableResult
    } else if e.is::<UnwritableOutputs>() {
        CompileLocallyReason::UnwritableOutputs
    } else {
        // `materialize` gives a type to every error except the CAS's.
        CompileLocallyReason::DownloadingOutputsFailed
    }
}

/// Returns `inline`, or the blob at `digest` when the server didn't inline it.
async fn download_stdout_or_stderr(
    cas: &Cas,
    inline: &[u8],
    digest: Option<&remexec::Digest>,
) -> Result<Vec<u8>> {
    match digest {
        Some(digest) if inline.is_empty() && digest.size_bytes > 0 => {
            cas.download_blob(digest, MAX_STDOUT_OR_STDERR_BYTES).await
        }
        _ => Ok(inline.to_vec()),
    }
}

/// Splits the worker root line off an action's stderr.
fn take_worker_root(stderr: &[u8]) -> Result<(String, Vec<u8>)> {
    let mut worker_root = None;
    let mut rest = Vec::with_capacity(stderr.len());
    for line in stderr.split_inclusive(|&b| b == b'\n') {
        match line.strip_prefix(WORKER_ROOT_MARKER.as_bytes()) {
            Some(reported) if worker_root.is_none() => {
                worker_root = Some(reported.trim_ascii_end());
            }
            _ => rest.extend_from_slice(line),
        }
    }
    // A worker root that isn't UTF-8 couldn't be found in outputs as written.
    let worker_root = worker_root
        .and_then(|r| std::str::from_utf8(r).ok())
        .filter(|r| !r.is_empty())
        .ok_or_else(|| UnusableResult("result doesn't report the worker root".to_owned()))?;
    Ok((worker_root.to_owned(), rest))
}

/// Fails if `result` has outputs other than the files `compile` declares, or lacks an rlib or
/// rmeta that a successful compile writes.
// Servers may also report symlinks in the fields REAPI 2.1 deprecated.
#[allow(deprecated)]
fn check_output_paths(compile: &ReapiCompile, result: &remexec::ActionResult) -> Result<()> {
    if !result.output_directories.is_empty()
        || !result.output_symlinks.is_empty()
        || !result.output_file_symlinks.is_empty()
        || !result.output_directory_symlinks.is_empty()
    {
        return Err(UnusableResult("result has output directories or symlinks".to_owned()).into());
    }
    let declared_paths: HashSet<String> = command_output_paths(compile).collect();
    let mut returned_paths = HashSet::new();
    for file in &result.output_files {
        if !declared_paths.contains(&file.path) {
            return Err(
                UnusableResult(format!("result has undeclared output {}", file.path)).into(),
            );
        }
        if !returned_paths.insert(file.path.as_str()) {
            return Err(UnusableResult(format!("result has output {} twice", file.path)).into());
        }
    }
    // rustc writes its rlib and rmeta when it succeeds, so a successful result without one
    // would leave a previous build's file at that path.
    if result.exit_code == 0 {
        for output in &compile.outputs {
            if output
                .extension()
                .is_some_and(|e| e == "rlib" || e == "rmeta")
                && let Some(path) = compile.path_mapper.to_input_root(output)
                && !returned_paths.contains(path.as_str())
            {
                return Err(UnusableResult(format!("result lacks output {path}")).into());
            }
        }
    }
    Ok(())
}

/// Downloads `file` next to its local path, or checks its inline contents, and fails if it
/// contains `worker_root`.
async fn download_output(
    cas: &Cas,
    compile: &ReapiCompile,
    file: &remexec::OutputFile,
    worker_root: &str,
) -> Result<(PathBuf, bool, OutputContents)> {
    let digest = file
        .digest
        .as_ref()
        .ok_or_else(|| UnusableResult(format!("result's output {} has no digest", file.path)))?;
    let local_path = compile.path_mapper.to_local(&file.path);
    if !file.contents.is_empty() {
        if digest::of_bytes(&file.contents) != *digest {
            return Err(UnusableResult(format!(
                "result's inline output {} doesn't match its digest",
                file.path
            ))
            .into());
        }
        if memmem::find(&file.contents, worker_root.as_bytes()).is_some() {
            return Err(WorkerRootInOutput(local_path).into());
        }
        let contents = OutputContents::Inline(file.contents.clone());
        return Ok((local_path, file.is_executable, contents));
    }
    let dir = local_path
        .parent()
        .with_context(|| format!("output {} has no parent directory", local_path.display()))
        .context(UnwritableOutputs)?;
    let temp_file = NamedTempFile::new_in(dir)
        .with_context(|| format!("failed to create a file in {}", dir.display()))
        .context(UnwritableOutputs)?;
    let temp_file_path = temp_file.path().to_owned();
    let scanner = WorkerRootScanner::new(io::BufWriter::new(temp_file), worker_root);
    let scanner = cas
        .download_blob_into(digest, MAX_OUTPUT_FILE_BYTES, scanner)
        .await?;
    if scanner.contains_worker_root {
        return Err(WorkerRootInOutput(local_path).into());
    }
    let temp_file = scanner
        .inner
        .into_inner()
        .map_err(|e| e.into_error())
        .with_context(|| format!("failed to write {}", temp_file_path.display()))
        .context(UnwritableOutputs)?;
    let contents = OutputContents::TempFile(temp_file.into_temp_path());
    Ok((local_path, file.is_executable, contents))
}

/// Moves each output to its local path, or removes the ones already moved if one fails.
fn write_outputs(output_contents: Vec<(PathBuf, bool, OutputContents)>) -> Result<()> {
    let mut written_paths = vec![];
    for (local_path, is_executable, contents) in output_contents {
        let write_result = match contents {
            OutputContents::TempFile(temp_path) => temp_path
                .persist(&local_path)
                .map_err(|e| Error::new(e.error))
                .with_context(|| format!("failed to write {}", local_path.display())),
            OutputContents::Inline(data) => write_output(&local_path, &data),
        }
        .and_then(|()| {
            written_paths.push(local_path.clone());
            set_mode(&local_path, is_executable)
        });
        if let Err(e) = write_result {
            remove_outputs(&written_paths);
            return Err(e.context(UnwritableOutputs));
        }
    }
    Ok(())
}

fn write_output(path: &Path, data: &[u8]) -> Result<()> {
    let dir = path
        .parent()
        .with_context(|| format!("output {} has no parent directory", path.display()))?;
    let mut temp_file = NamedTempFile::new_in(dir)
        .with_context(|| format!("failed to create a file in {}", dir.display()))?;
    temp_file
        .write_all(data)
        .with_context(|| format!("failed to write {}", path.display()))?;
    temp_file
        .persist(path)
        .map_err(|e| Error::new(e.error))
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

/// Sets an output's mode to what a local compile gives it under umask 022.
fn set_mode(path: &Path, is_executable: bool) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = if is_executable { 0o755 } else { 0o644 };
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
            .with_context(|| format!("failed to set the mode of {}", path.display()))?;
    }
    #[cfg(not(unix))]
    let _ = (path, is_executable);
    Ok(())
}

/// Removes `paths`, logging any that can't be removed.
fn remove_outputs(paths: &[PathBuf]) {
    for path in paths {
        if let Err(e) = std::fs::remove_file(path) {
            warn!("REAPI: failed to remove the output {}: {e}", path.display());
        }
    }
}

/// The status of a process that exited with `exit_code`.
fn exit_status_from_code(exit_code: u8) -> process::ExitStatus {
    // A Unix wait status holds the exit code in bits 8 to 15.
    #[cfg(unix)]
    let raw_status = i32::from(exit_code) << 8;
    #[cfg(windows)]
    let raw_status = u32::from(exit_code);
    exit_status(raw_status)
}

#[cfg(test)]
pub(super) mod test {
    use super::*;
    use crate::reapi::fake::FakeServer;
    use crate::reapi::{ActionEnv, PathMapper, ToolchainKey};
    use std::sync::Arc;
    use test_case::test_case;

    const RLIB: &str = ".sccache-build/debug/deps/libfoo.rlib";
    const RMETA: &str = ".sccache-build/debug/deps/libfoo.rmeta";

    /// A compile in `cwd` that declares an rmeta and an rlib.
    fn reapi_compile_in(cwd: &str) -> ReapiCompile {
        ReapiCompile {
            path_mapper: PathMapper::new(cwd.into(), format!("{cwd}/target").into()),
            arguments: vec!["--crate-name=foo".to_owned()],
            mapped_crate_root: Some("src/lib.rs".to_owned()),
            toolchain: ToolchainKey::new(
                "abc123".to_owned(),
                "/toolchain".into(),
                "x86_64-unknown-linux-gnu".to_owned(),
            ),
            env: ActionEnv {
                vars: vec![
                    ("CARGO_PKG_VERSION".to_owned(), "1.0.0".to_owned()),
                    ("CARGO_CRATE_NAME".to_owned(), "foo".to_owned()),
                ],
                path_vars: vec![
                    (
                        "OUT_DIR".to_owned(),
                        ".sccache-build/debug/build/foo-1/out".to_owned(),
                    ),
                    ("CARGO_MANIFEST_DIR".to_owned(), ".".to_owned()),
                ],
                unset_vars: vec!["HOSTNAME".to_owned(), "GIT_HASH".to_owned()],
            },
            input_root_files: vec![],
            outputs: vec![
                format!("{cwd}/target/debug/deps/libfoo.rmeta").into(),
                format!("{cwd}/target/debug/deps/libfoo.rlib").into(),
            ],
        }
    }

    #[test]
    fn test_the_command_doesnt_depend_on_the_checkout_path() {
        assert_eq!(
            build_command(&reapi_compile_in("/home/a/ws")),
            build_command(&reapi_compile_in("/srv/b/ws"))
        );
    }

    /// The action digest of a compile of a `src/lib.rs` holding `lib_rs` in a new checkout
    /// under `dir`, with the toolchain at `sysroot`, changed by `change`.
    fn action_digest_in(
        dir: &Path,
        lib_rs: &str,
        sysroot: &Path,
        change: fn(&mut ReapiCompile),
        options: &ActionOptions,
    ) -> remexec::Digest {
        action_blobs_in(dir, lib_rs, sysroot, change, options).action_digest
    }

    fn action_blobs_in(
        dir: &Path,
        lib_rs: &str,
        sysroot: &Path,
        change: fn(&mut ReapiCompile),
        options: &ActionOptions,
    ) -> ActionBlobs {
        let cwd = dir.join("ws");
        let lib_rs_path = cwd.join("src/lib.rs");
        std::fs::create_dir_all(lib_rs_path.parent().unwrap()).unwrap();
        std::fs::write(&lib_rs_path, lib_rs).unwrap();
        let mut compile = reapi_compile_in(cwd.to_str().unwrap());
        compile.input_root_files = vec![lib_rs_path];
        let digest_cache = DigestCache::load(dir.join("digests.json"));
        change(&mut compile);
        let files = digest_input_root_files(&compile, &digest_cache).unwrap();
        let toolchain =
            toolchain_files(sysroot, "x86_64-unknown-linux-gnu", &digest_cache).unwrap();
        action_blobs(&compile, &files, &toolchain, options).unwrap()
    }

    #[cfg(unix)]
    #[test]
    fn test_the_action_blobs_hold_every_blob_the_action_needs() {
        let dir = tempfile::tempdir().unwrap();
        let sysroot = fake_sysroot(&dir.path().join("sysroot"));
        let action_blobs = action_blobs_in(
            dir.path(),
            "pub fn f() {}",
            &sysroot,
            |_| {},
            &ActionOptions::default(),
        );
        let blobs: std::collections::HashMap<_, _> = action_blobs.blobs.into_iter().collect();
        let read = |digest: &remexec::Digest| {
            let bytes = match &blobs[digest] {
                Blob::Bytes(bytes) => bytes.clone(),
                Blob::File(path) => std::fs::read(path).unwrap(),
            };
            assert_eq!(*digest, digest::of_bytes(&bytes));
            bytes
        };
        let action = remexec::Action::decode(read(&action_blobs.action_digest).as_slice()).unwrap();
        remexec::Command::decode(read(action.command_digest.as_ref().unwrap()).as_slice()).unwrap();
        let mut directories = vec![action.input_root_digest.unwrap()];
        let mut file_names = vec![];
        while let Some(digest) = directories.pop() {
            let directory = remexec::Directory::decode(read(&digest).as_slice()).unwrap();
            for file in directory.files {
                read(file.digest.as_ref().unwrap());
                file_names.push(file.name);
            }
            directories.extend(directory.directories.into_iter().map(|d| d.digest.unwrap()));
        }
        assert!(file_names.contains(&"lib.rs".to_owned()), "{file_names:?}");
        assert!(file_names.contains(&"rustc".to_owned()), "{file_names:?}");
    }

    #[cfg(unix)]
    #[test]
    fn test_the_action_depends_on_the_compile_but_not_the_checkout_path() {
        let toolchains = tempfile::tempdir().unwrap();
        let sysroot = fake_sysroot(&toolchains.path().join("a"));
        // fake_sysroot writes each file's path into it, so this toolchain's files differ.
        let other_sysroot = fake_sysroot(&toolchains.path().join("b"));
        let digest = |lib_rs, sysroot: &Path, change: fn(&mut ReapiCompile)| {
            let checkout = tempfile::tempdir().unwrap();
            action_digest_in(
                checkout.path(),
                lib_rs,
                sysroot,
                change,
                &ActionOptions::default(),
            )
        };
        let unchanged: fn(&mut ReapiCompile) = |_| {};
        let first = digest("pub fn f() {}", &sysroot, unchanged);
        assert_eq!(
            first,
            digest("pub fn f() {}", &sysroot, unchanged),
            "another checkout"
        );
        assert_ne!(
            first,
            digest("pub fn g() {}", &sysroot, unchanged),
            "another lib.rs"
        );
        assert_ne!(
            first,
            digest("pub fn f() {}", &other_sysroot, unchanged),
            "another toolchain"
        );
        assert_ne!(
            first,
            digest("pub fn f() {}", &sysroot, |compile| {
                compile.arguments.push("-Copt-level=3".to_owned());
            }),
            "another argument"
        );
        assert_ne!(
            first,
            digest("pub fn f() {}", &sysroot, |compile| {
                compile.env.vars[0].1 = "2.0.0".to_owned();
            }),
            "another env var value"
        );
    }

    fn config_with_salt_and_platform() -> ReapiConfig {
        ReapiConfig {
            salt: "1".to_owned(),
            platform: [("OSFamily".to_owned(), "linux".to_owned())].into(),
            ..Default::default()
        }
    }

    #[cfg(unix)]
    #[test]
    fn test_the_action_depends_on_the_salt_and_the_platform() {
        let dir = tempfile::tempdir().unwrap();
        let sysroot = fake_sysroot(&dir.path().join("sysroot"));
        let digest = |change: fn(&mut ReapiConfig)| {
            let mut config = config_with_salt_and_platform();
            change(&mut config);
            let checkout = tempfile::tempdir().unwrap();
            action_digest_in(
                checkout.path(),
                "pub fn f() {}",
                &sysroot,
                |_| {},
                &ActionOptions::new(&config),
            )
        };
        let first = digest(|_| {});
        assert_eq!(first, digest(|_| {}), "the same options");
        assert_ne!(
            first,
            digest(|config| config.salt.push('2')),
            "another salt"
        );
        assert_ne!(
            first,
            digest(|config| {
                config
                    .platform
                    .insert("OSFamily".to_owned(), "windows".to_owned());
            }),
            "another platform property value"
        );
        assert_ne!(
            first,
            digest(|config| config.platform.clear()),
            "no platform"
        );
    }

    #[test]
    fn test_the_command_and_the_action_carry_the_platform() {
        let options = ActionOptions::new(&config_with_salt_and_platform());
        let (command, action) =
            build_action(&reapi_compile_in("/ws"), digest::of_bytes(b""), &options);
        let platform = Some(remexec::Platform {
            properties: vec![remexec::platform::Property {
                name: "OSFamily".to_owned(),
                value: "linux".to_owned(),
            }],
        });
        #[expect(deprecated)]
        let command_platform = command.platform;
        assert_eq!(platform, command_platform);
        assert_eq!(platform, action.platform);
    }

    #[test]
    fn test_the_command_sorts_its_outputs_and_vars() {
        let command = build_command(&reapi_compile_in("/ws"));
        assert_eq!([RLIB, RMETA], command.output_paths.as_slice());
        let var_names: Vec<&str> = command
            .environment_variables
            .iter()
            .map(|var| var.name.as_str())
            .collect();
        assert_eq!(
            ["CARGO_CRATE_NAME", "CARGO_PKG_VERSION"],
            var_names.as_slice()
        );
    }

    #[test]
    fn test_the_script_sets_path_vars_under_the_worker_root() {
        let command = build_command(&reapi_compile_in("/ws"));
        let script = &command.arguments[2];
        assert!(
            script.contains(
                "unset GIT_HASH HOSTNAME\n\
                 export CARGO_MANIFEST_DIR=\"$PWD\"/'.'\n\
                 export OUT_DIR=\"$PWD\"/'.sccache-build/debug/build/foo-1/out'\n"
            ),
            "{script}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_the_command_runs_rustc_with_worker_paths() {
        use std::os::unix::fs::PermissionsExt;
        let worker_root = tempfile::tempdir().unwrap();
        let worker_root = worker_root.path().canonicalize().unwrap();
        let rustc = worker_root.join(".sccache-toolchain/bin/rustc");
        std::fs::create_dir_all(rustc.parent().unwrap()).unwrap();
        // A fake rustc in a shell that doesn't set GIT_HASH itself, as bash does HOSTNAME.
        std::fs::write(
            &rustc,
            "#!/bin/sh\nprintf '%s\\n' \"$@\" \"OUT_DIR=$OUT_DIR\" \"GIT_HASH=${GIT_HASH-unset}\" \
             \"LD_LIBRARY_PATH=$LD_LIBRARY_PATH\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&rustc, std::fs::Permissions::from_mode(0o755)).unwrap();
        let command = build_command(&reapi_compile_in("/home/a/ws"));
        let output = std::process::Command::new(&command.arguments[0])
            .args(&command.arguments[1..])
            .current_dir(&worker_root)
            .env("GIT_HASH", "abc")
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        let w = worker_root.display();
        assert_eq!(
            format!(
                "--sysroot\n{w}/.sccache-toolchain\n--crate-name=foo\n{w}/src/lib.rs\n\
                 --remap-path-prefix={w}=.\n\
                 --remap-path-prefix={w}/.sccache-build=.sccache-build\n\
                 OUT_DIR={w}/.sccache-build/debug/build/foo-1/out\nGIT_HASH=unset\n\
                 LD_LIBRARY_PATH={w}/.sccache-toolchain/lib\n"
            ),
            String::from_utf8(output.stdout).unwrap()
        );
        assert_eq!(
            format!("{WORKER_ROOT_MARKER}{w}\n"),
            String::from_utf8(output.stderr).unwrap()
        );
    }

    #[test]
    fn test_sh_quote() {
        assert_eq!(r"'it'\''s'", sh_quote("it's"));
    }

    fn cas_output(path: &str, data: &[u8]) -> remexec::OutputFile {
        remexec::OutputFile {
            path: path.to_owned(),
            digest: Some(digest::of_bytes(data)),
            ..Default::default()
        }
    }

    pub(in crate::reapi) fn inline_output(path: &str, data: &[u8]) -> remexec::OutputFile {
        remexec::OutputFile {
            contents: data.to_vec(),
            ..cas_output(path, data)
        }
    }

    /// A successful result with `output_files` from a worker whose worker root is /w/1.
    pub(in crate::reapi) fn worker_result(
        output_files: Vec<remexec::OutputFile>,
    ) -> remexec::ActionResult {
        remexec::ActionResult {
            output_files,
            stderr_raw: format!("{WORKER_ROOT_MARKER}/w/1\nwarning: in /w/1/src/lib.rs\n")
                .into_bytes(),
            ..Default::default()
        }
    }

    /// A successful result with the rlib and rmeta from the CAS.
    fn rlib_and_rmeta_result() -> remexec::ActionResult {
        worker_result(vec![cas_output(RLIB, b"rlib"), cas_output(RMETA, b"rmeta")])
    }

    /// Materializes `result` for `reapi_compile_in(dir)`, with `blobs` in the fake's CAS.
    async fn materialize_in(
        dir: &Path,
        blobs: &[&[u8]],
        result: remexec::ActionResult,
    ) -> Result<process::Output> {
        // Cargo creates the output directory before running rustc.
        std::fs::create_dir_all(dir.join("target/debug/deps")).unwrap();
        let server = Arc::new(FakeServer::default());
        for blob in blobs {
            server.insert_blob(blob);
        }
        let cas = Cas::new(&server.serve_and_connect().await);
        materialize(&cas, &reapi_compile_in(dir.to_str().unwrap()), &result).await
    }

    /// The names of the files in `dir`'s deps directory. A download that another output's
    /// failure cancels removes its temp file when its write task ends, which may be later.
    fn output_names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir.join("target/debug/deps"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .filter(|name| !name.starts_with(".tmp"))
            .collect();
        names.sort();
        names
    }

    #[test_case(cas_output ; "from the CAS")]
    #[test_case(inline_output ; "inline")]
    #[tokio::test]
    async fn test_outputs_are_written_to_their_local_paths(
        output: fn(&str, &[u8]) -> remexec::OutputFile,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let files = vec![output(RLIB, b"rlib"), output(RMETA, b"rmeta")];
        let output = materialize_in(dir.path(), &[b"rlib", b"rmeta"], worker_result(files))
            .await
            .unwrap();
        assert!(output.status.success());
        let deps = dir.path().join("target/debug/deps");
        assert_eq!(
            b"rlib".as_slice(),
            std::fs::read(deps.join("libfoo.rlib")).unwrap()
        );
        assert_eq!(
            b"rmeta".as_slice(),
            std::fs::read(deps.join("libfoo.rmeta")).unwrap()
        );
        // No temp files are left behind.
        assert_eq!(2, std::fs::read_dir(deps).unwrap().count());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_outputs_get_a_local_compiles_mode() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let mut result = rlib_and_rmeta_result();
        result.output_files[0].is_executable = true;
        materialize_in(dir.path(), &[b"rlib", b"rmeta"], result)
            .await
            .unwrap();
        let deps = dir.path().join("target/debug/deps");
        let mode = |name: &str| {
            std::fs::metadata(deps.join(name))
                .unwrap()
                .permissions()
                .mode()
                & 0o777
        };
        assert_eq!(0o755, mode("libfoo.rlib"));
        assert_eq!(0o644, mode("libfoo.rmeta"));
    }

    #[tokio::test]
    async fn test_stderr_has_local_paths_without_the_worker_root_line() {
        let dir = tempfile::tempdir().unwrap();
        let output = materialize_in(dir.path(), &[b"rlib", b"rmeta"], rlib_and_rmeta_result())
            .await
            .unwrap();
        assert_eq!(
            format!("warning: in {}\n", dir.path().join("src/lib.rs").display()),
            String::from_utf8(output.stderr).unwrap()
        );
    }

    #[tokio::test]
    async fn test_stdout_and_stderr_can_come_from_the_cas() {
        let dir = tempfile::tempdir().unwrap();
        let stdout = b"in /w/1/src/lib.rs";
        let stderr = format!("{WORKER_ROOT_MARKER}/w/1\n");
        let result = remexec::ActionResult {
            exit_code: 1,
            stdout_digest: Some(digest::of_bytes(stdout)),
            stderr_raw: vec![],
            stderr_digest: Some(digest::of_bytes(stderr.as_bytes())),
            ..worker_result(vec![])
        };
        let output = materialize_in(dir.path(), &[stdout, stderr.as_bytes()], result)
            .await
            .unwrap();
        assert_eq!(
            format!("in {}", dir.path().join("src/lib.rs").display()),
            String::from_utf8(output.stdout).unwrap()
        );
        assert!(output.stderr.is_empty());
    }

    #[tokio::test]
    async fn test_a_failed_compile_returns_its_exit_code() {
        let dir = tempfile::tempdir().unwrap();
        let result = remexec::ActionResult {
            exit_code: 1,
            ..worker_result(vec![])
        };
        let output = materialize_in(dir.path(), &[], result).await.unwrap();
        assert_eq!(Some(1), output.status.code());
    }

    #[test_case(
        remexec::ActionResult { exit_code: 256, ..worker_result(vec![]) },
        "exit code 256" ; "exit code a process can't return")]
    #[test_case(
        worker_result(vec![cas_output(RLIB, b"rlib"), cas_output(RMETA, b"rmeta"), inline_output("other", b"x")]),
        "result has undeclared output other" ; "undeclared output")]
    #[test_case(
        worker_result(vec![cas_output(RLIB, b"rlib"), cas_output(RMETA, b"rmeta"), inline_output("../x", b"x")]),
        "result has undeclared output ../x" ; "output outside the input root")]
    #[test_case(
        worker_result(vec![cas_output(RLIB, b"rlib"), cas_output(RMETA, b"rmeta"), cas_output(RLIB, b"rmeta")]),
        "twice" ; "output returned twice")]
    #[test_case(
        remexec::ActionResult {
            output_symlinks: vec![remexec::OutputSymlink { path: RLIB.to_owned(), target: "/etc/passwd".to_owned(), ..Default::default() }],
            ..worker_result(vec![cas_output(RMETA, b"rmeta")])
        },
        "symlinks" ; "output symlink")]
    #[test_case(
        #[allow(deprecated)]
        remexec::ActionResult {
            output_file_symlinks: vec![remexec::OutputSymlink { path: RLIB.to_owned(), target: "/etc/passwd".to_owned(), ..Default::default() }],
            ..worker_result(vec![cas_output(RMETA, b"rmeta")])
        },
        "symlinks" ; "output symlink in a deprecated field")]
    #[test_case(
        remexec::ActionResult {
            output_directories: vec![remexec::OutputDirectory { path: ".sccache-build".to_owned(), ..Default::default() }],
            ..rlib_and_rmeta_result()
        },
        "directories" ; "output directory")]
    #[test_case(
        worker_result(vec![cas_output(RMETA, b"rmeta")]),
        "result lacks output .sccache-build/debug/deps/libfoo.rlib" ; "missing rlib")]
    #[test_case(
        worker_result(vec![remexec::OutputFile { digest: None, ..cas_output(RLIB, b"rlib") }, cas_output(RMETA, b"rmeta")]),
        "has no digest" ; "output without a digest")]
    #[test_case(
        worker_result(vec![
            remexec::OutputFile { digest: Some(digest::of_bytes(b"other")), ..inline_output(RLIB, b"rlib") },
            cas_output(RMETA, b"rmeta"),
        ]),
        "doesn't match its digest" ; "inline output that doesn't match its digest")]
    #[test_case(
        remexec::ActionResult { stderr_raw: vec![], ..rlib_and_rmeta_result() },
        "doesn't report the worker root" ; "no worker root line")]
    #[test_case(
        remexec::ActionResult {
            stderr_raw: format!("{WORKER_ROOT_MARKER}\n").into_bytes(),
            ..rlib_and_rmeta_result()
        },
        "doesn't report the worker root" ; "empty worker root")]
    #[test_case(
        remexec::ActionResult {
            stderr_raw: [WORKER_ROOT_MARKER.as_bytes(), b"/w/\xff\n"].concat(),
            ..rlib_and_rmeta_result()
        },
        "doesn't report the worker root" ; "worker root that isn't UTF-8")]
    #[tokio::test]
    async fn test_unusable_results_write_nothing(result: remexec::ActionResult, expected: &str) {
        let dir = tempfile::tempdir().unwrap();
        let e = materialize_in(dir.path(), &[b"rlib", b"rmeta"], result)
            .await
            .unwrap_err();
        assert!(e.is::<UnusableResult>(), "{e:#}");
        assert!(format!("{e:#}").contains(expected), "{e:#}");
        assert!(output_names(dir.path()).is_empty());
    }

    #[tokio::test]
    async fn test_an_output_the_cas_doesnt_have_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let files = vec![
            cas_output(RLIB, b"rlib"),
            cas_output(RMETA, b"not in the CAS"),
        ];
        let e = materialize_in(dir.path(), &[b"rlib"], worker_result(files))
            .await
            .unwrap_err();
        assert!(format!("{e:#}").contains("NotFound"), "{e:#}");
        assert!(output_names(dir.path()).is_empty());
    }

    #[test_case(cas_output(RLIB, b"in /w/1/src") ; "from the CAS")]
    #[test_case(inline_output(RLIB, b"in /w/1/src") ; "inline")]
    #[tokio::test]
    async fn test_an_output_containing_the_worker_root_writes_nothing(rlib: remexec::OutputFile) {
        let dir = tempfile::tempdir().unwrap();
        // The rmeta comes first, so writing it before checking the rlib would leave it behind.
        let files = vec![cas_output(RMETA, b"rmeta"), rlib];
        let e = materialize_in(
            dir.path(),
            &[b"in /w/1/src", b"rmeta"],
            worker_result(files),
        )
        .await
        .unwrap_err();
        assert!(e.is::<WorkerRootInOutput>(), "{e:#}");
        assert!(output_names(dir.path()).is_empty());
    }

    #[tokio::test]
    async fn test_a_failed_write_removes_the_outputs_already_written() {
        let dir = tempfile::tempdir().unwrap();
        // A non-empty directory at the rmeta's path, which a rename can't replace.
        std::fs::create_dir_all(dir.path().join("target/debug/deps/libfoo.rmeta/x")).unwrap();
        let e = materialize_in(dir.path(), &[b"rlib", b"rmeta"], rlib_and_rmeta_result())
            .await
            .unwrap_err();
        assert!(e.is::<UnwritableOutputs>(), "{e:#}");
        assert_eq!(["libfoo.rmeta"], output_names(dir.path()).as_slice());
    }

    #[test]
    fn test_the_worker_root_line_is_taken_once() {
        let stderr = format!("{WORKER_ROOT_MARKER}/w/1\nwarning: x\n{WORKER_ROOT_MARKER}/w/2\n");
        let (worker_root, rest) = take_worker_root(stderr.as_bytes()).unwrap();
        assert_eq!("/w/1", worker_root);
        assert_eq!(
            format!("warning: x\n{WORKER_ROOT_MARKER}/w/2\n"),
            String::from_utf8(rest).unwrap()
        );
    }

    #[test]
    fn test_a_worker_root_split_across_writes_is_found() {
        let contains_worker_root = |writes: &[&[u8]]| {
            let mut scanner = WorkerRootScanner::new(vec![], "/w/1");
            for write in writes {
                scanner.write_all(write).unwrap();
            }
            assert_eq!(writes.concat(), scanner.inner);
            scanner.contains_worker_root
        };
        assert!(contains_worker_root(&[b"in /", b"w", b"/1/src"]));
        assert!(contains_worker_root(&[b"x/w/1"]));
        assert!(contains_worker_root(&[b"/", b"w", b"/", b"1"]));
        assert!(contains_worker_root(&[b"a/w", b"/1"]));
        assert!(!contains_worker_root(&[b"in /w", b"/2/1"]));
    }

    /// A sysroot in `dir` with rustc, its libraries, a sanitizer runtime, and std and
    /// rust-lld for x86_64-unknown-linux-gnu.
    pub(in crate::reapi) fn fake_sysroot(dir: &Path) -> PathBuf {
        let sysroot = dir.join("sysroot");
        for path in [
            "bin/rustc",
            "lib/librustc_driver-x.so",
            "lib/libLLVM.so.1",
            "lib/librustc-stable_rt.asan.so",
            "lib/rustlib/x86_64-unknown-linux-gnu/lib/libstd-x.rlib",
            "lib/rustlib/x86_64-unknown-linux-gnu/bin/rust-lld",
        ] {
            let path = sysroot.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, path.to_str().unwrap()).unwrap();
        }
        sysroot
    }

    #[cfg(unix)]
    #[test]
    fn test_toolchain_files_are_rustc_its_libraries_std_and_linker() {
        let dir = tempfile::tempdir().unwrap();
        let sysroot = fake_sysroot(dir.path());
        let target_triple_dir = "lib/rustlib/x86_64-unknown-linux-gnu";
        // A std file symlinked from outside the sysroot.
        let linked = dir.path().join("store/libcore-x.rlib");
        std::fs::create_dir_all(linked.parent().unwrap()).unwrap();
        std::fs::write(&linked, b"core").unwrap();
        std::os::unix::fs::symlink(
            &linked,
            sysroot.join(target_triple_dir).join("lib/libcore-x.rlib"),
        )
        .unwrap();
        let digest_cache = DigestCache::load(dir.path().join("digests"));

        let mut files: Vec<(String, bool)> =
            toolchain_files(&sysroot, "x86_64-unknown-linux-gnu", &digest_cache)
                .unwrap()
                .into_iter()
                .map(|(file, _)| (file.path, file.is_executable))
                .collect();
        files.sort();
        let expected = [
            (".sccache-toolchain/bin/rustc".to_owned(), true),
            (".sccache-toolchain/lib/libLLVM.so.1".to_owned(), false),
            (
                ".sccache-toolchain/lib/librustc_driver-x.so".to_owned(),
                false,
            ),
            (
                format!(".sccache-toolchain/{target_triple_dir}/bin/rust-lld"),
                true,
            ),
            (
                format!(".sccache-toolchain/{target_triple_dir}/lib/libcore-x.rlib"),
                false,
            ),
            (
                format!(".sccache-toolchain/{target_triple_dir}/lib/libstd-x.rlib"),
                false,
            ),
        ];
        assert_eq!(expected.as_slice(), files.as_slice());
    }

    #[test]
    fn test_a_sysroot_without_the_targets_std_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let sysroot = fake_sysroot(dir.path());
        let digest_cache = DigestCache::load(dir.path().join("digests"));
        let e = toolchain_files(&sysroot, "aarch64-apple-darwin", &digest_cache).unwrap_err();
        assert!(
            e.to_string().contains("no std for aarch64-apple-darwin"),
            "{e:#}"
        );
        assert!(matches!(
            e.downcast_ref(),
            Some(UnusableToolchain(CompileLocallyReason::MissingRustcOrStd))
        ));
    }

    #[test]
    fn test_a_sysroot_without_rustc_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let sysroot = fake_sysroot(dir.path());
        std::fs::remove_file(sysroot.join("bin/rustc")).unwrap();
        let digest_cache = DigestCache::load(dir.path().join("digests"));
        let e = toolchain_files(&sysroot, "x86_64-unknown-linux-gnu", &digest_cache).unwrap_err();
        assert!(e.to_string().contains("no rustc"), "{e:#}");
        assert!(matches!(
            e.downcast_ref(),
            Some(UnusableToolchain(CompileLocallyReason::MissingRustcOrStd))
        ));
    }
}
