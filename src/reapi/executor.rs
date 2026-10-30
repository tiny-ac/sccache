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

//! Looks up the cache misses of a build with `[dist.reapi]` in the REAPI server's action
//! cache, and executes the ones it doesn't have on the server's workers, or locally with the
//! arguments a REAPI worker would use. The stats count the hits, the executions, and the
//! local compiles by reason.

use super::cas::UnreadableBlob;
use super::client::{ConnectOptions, RPC_TIMEOUT, check_connect_options, rpc_code};
use super::connection::{Connection, ServerApis};
use super::digest::DigestCache;
use super::execution::{Execution, MissingActionBlobs, QueuedTooLong};
use super::mapping::{CompileLocallyReason, ReapiCompile, ToolchainKey};
use super::pause::Pause;
use super::proto::build::bazel::remote::execution::v2 as remexec;
use super::remote_compile::{
    ActionBlobs, ActionOptions, action_blobs, digest_input_root_files, download_stdout_or_stderr,
    materialize, materialize_failure_reason, rustc_version_action_blobs,
};
use super::toolchains::{ToolchainFiles, Toolchains};
use crate::compiler::{
    CacheControl, Compilation, DistInputOptions, DistType, LocalCompile, LocalCompileChanges,
    ReapiOutcome, RemoteExecutor, RemoteJob,
};
use crate::config::{ReapiConfig, ReapiOnMiss};
use crate::errors::*;
use crate::mock_command::CommandCreatorSync;
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Directory under the dist cache dir for the sysroot copies local compiles use.
const SYSROOT_COPIES_DIR: &str = "reapi-sysroot-copies";
/// File under the dist cache dir that keeps toolchain file digests across restarts.
const DIGEST_CACHE_FILE: &str = "reapi-digests.json";
/// Bounds an execution, with its reattaches and its retry after uploading again.
const EXECUTE_TIMEOUT: Duration = Duration::from_secs(600);
/// How long a miss the server reports queued waits for a worker without `queue_timeout_secs`.
const DEFAULT_QUEUE_TIMEOUT: Duration = Duration::from_secs(60);
/// Caps `ready_wait_secs`, since `Instant::now() + ready_wait` panics on overflow.
const MAX_READY_WAIT: Duration = Duration::from_secs(3600);

/// A [`RemoteExecutor`] for a REAPI server.
pub struct ReapiExecutor {
    /// `None` when `[dist.reapi]` is misconfigured.
    connection: Option<Arc<Connection>>,
    digest_cache: Arc<DigestCache>,
    toolchains: Arc<Toolchains>,
    ready_wait: Duration,
    sysroot_copies_dir: PathBuf,
    lookup_pause: Pause,
    execution_pause: Pause,
    action_options: ActionOptions,
    on_miss: ReapiOnMiss,
    queue_timeout: Duration,
    is_linux_client: bool,
    rustc_checks: Mutex<HashMap<ToolchainKey, Arc<RustcCheck>>>,
    /// Set when the server refuses an execution, so misses compile locally until the sccache
    /// server restarts.
    executions_refused: AtomicBool,
}

/// The check, once per toolchain, that the workers run its rustc.
#[derive(Default)]
struct RustcCheck {
    outcome: tokio::sync::OnceCell<RustcOnWorkers>,
    /// How many checks failed to reach a worker, and the last one's reason, so a compile that
    /// waited for a check that failed doesn't start another.
    failures: Mutex<(u64, Option<CompileLocallyReason>)>,
}

#[derive(Clone, Copy)]
enum RustcOnWorkers {
    Matches,
    /// Compiles with the toolchain compile locally, for this reason.
    Unusable(CompileLocallyReason),
}

/// What looking a compile up, and executing it on a miss, needs.
struct Lookup {
    reapi_compile: ReapiCompile,
    apis: Arc<ServerApis>,
    toolchain: ToolchainFiles,
}

impl ReapiExecutor {
    /// Reads the auth token once, so a changed token takes a restart.
    pub fn new(config: &ReapiConfig, dist_cache_dir: &Path) -> Self {
        let connection = match read_connect_options(config) {
            Ok(options) => Some(Connection::new(options)),
            Err(e) => {
                warn!("REAPI: {e:#}; every compile runs locally");
                None
            }
        };
        let mut ready_wait = Duration::from_secs(config.ready_wait_secs);
        if ready_wait > MAX_READY_WAIT {
            warn!(
                "REAPI: ready_wait_secs is capped at {}",
                MAX_READY_WAIT.as_secs()
            );
            ready_wait = MAX_READY_WAIT;
        }
        let digest_cache = Arc::new(DigestCache::load(dist_cache_dir.join(DIGEST_CACHE_FILE)));
        ReapiExecutor {
            connection,
            toolchains: Toolchains::new(digest_cache.clone()),
            digest_cache,
            ready_wait,
            sysroot_copies_dir: dist_cache_dir.join(SYSROOT_COPIES_DIR),
            lookup_pause: Pause::new(RPC_TIMEOUT),
            execution_pause: Pause::new(EXECUTE_TIMEOUT),
            action_options: ActionOptions::new(config),
            on_miss: config.on_miss,
            queue_timeout: config
                .queue_timeout_secs
                .map_or(DEFAULT_QUEUE_TIMEOUT, Duration::from_secs),
            is_linux_client: cfg!(target_os = "linux"),
            rustc_checks: Default::default(),
            executions_refused: AtomicBool::new(false),
        }
    }

    /// What looking `compilation` up needs, or why it compiles locally.
    async fn prepare_lookup<T: CommandCreatorSync>(
        &self,
        compilation: Box<dyn Compilation<T>>,
        cache_control: CacheControl,
        local_compile_changed: bool,
        out_pretty: &str,
    ) -> std::result::Result<Lookup, CompileLocallyReason> {
        // SCCACHE_RECACHE and SCCACHE_NO_CACHE ask for a real compile, which a hit would skip.
        if cache_control != CacheControl::Default {
            return compile_locally(out_pretty, CompileLocallyReason::CacheBypassed);
        }
        let mapped = tokio::task::spawn_blocking(move || compilation.map_for_reapi()).await;
        let reapi_compile = match mapped {
            Ok(Ok(reapi_compile)) => reapi_compile,
            Ok(Err(not_eligible)) => {
                match &not_eligible.detail {
                    Some(detail) => debug!(
                        "[{}]: Compiling locally: {}: {:#}",
                        out_pretty, not_eligible.reason, detail
                    ),
                    None => debug!(
                        "[{}]: Compiling locally: {}",
                        out_pretty, not_eligible.reason
                    ),
                }
                return Err(not_eligible.reason);
            }
            // A panic is a bug in sccache, which shouldn't fail a compile that runs locally.
            Err(e) => {
                warn!(
                    "[{}]: Compiling locally: failed to map the compile for REAPI: {}",
                    out_pretty, e
                );
                return Err(CompileLocallyReason::MappingPanicked);
            }
        };
        // sccache caches a hit under the hash key of the changed local compile.
        if !local_compile_changed {
            return compile_locally(out_pretty, CompileLocallyReason::LocalCompileUnchanged);
        }
        let Some(connection) = &self.connection else {
            return compile_locally(out_pretty, CompileLocallyReason::Misconfigured);
        };
        if !self.lookup_pause.may_call() {
            return compile_locally(out_pretty, CompileLocallyReason::ActionCacheLookupsPaused);
        }
        let deadline = Instant::now() + self.ready_wait;
        let ready = tokio::try_join!(
            connection.server_apis(deadline),
            self.toolchains.files(&reapi_compile.toolchain, deadline)
        );
        match ready {
            Ok((apis, toolchain)) => Ok(Lookup {
                reapi_compile,
                apis,
                toolchain,
            }),
            Err(reason) => compile_locally(out_pretty, reason),
        }
    }

    /// Looks `lookup`'s compile up in the action cache and, on a miss, executes it unless
    /// `on_miss` is local, then writes its outputs.
    async fn look_up_or_execute(
        &self,
        lookup: Lookup,
        out_pretty: &str,
    ) -> std::result::Result<(ReapiOutcome, process::Output), CompileLocallyReason> {
        let Lookup {
            reapi_compile,
            apis,
            toolchain,
        } = lookup;
        let digest_cache = self.digest_cache.clone();
        let action_options = self.action_options.clone();
        let toolchain_files = toolchain.clone();
        let digested = tokio::task::spawn_blocking(move || {
            let files = digest_input_root_files(&reapi_compile, &digest_cache)?;
            let action = action_blobs(&reapi_compile, &files, &toolchain, &action_options)?;
            Ok::<_, Error>((reapi_compile, action))
        })
        .await;
        let (reapi_compile, action) = match digested.map_err(Error::from).flatten() {
            Ok(digested) => digested,
            Err(e) => {
                warn!(
                    "[{}]: Compiling locally: failed to hash the inputs: {:#}",
                    out_pretty, e
                );
                return Err(CompileLocallyReason::HashingInputsFailed);
            }
        };
        let cached = match apis
            .action_cache
            .get_action_result(&action.action_digest)
            .await
        {
            Ok(cached) => {
                self.lookup_pause.call_succeeded();
                cached
            }
            Err(e) => {
                warn!("[{}]: Compiling locally: {:#}", out_pretty, e);
                if is_refused_read(&e) {
                    return Err(CompileLocallyReason::CredentialsRefused);
                }
                if let Some(pause) = self.lookup_pause.call_failed() {
                    warn!(
                        "REAPI: action cache lookups are failing; compiling locally for {pause:?}"
                    );
                }
                return Err(CompileLocallyReason::ActionCacheLookupFailed);
            }
        };
        let skip_cache_lookup = match cached {
            Some(result) if result.exit_code == 0 => {
                let output = write_outputs(
                    &apis,
                    &reapi_compile,
                    &result,
                    &action.action_digest,
                    out_pretty,
                )
                .await?;
                debug!(
                    "[{}]: REAPI action cache hit {}",
                    out_pretty, action.action_digest.hash
                );
                return Ok((ReapiOutcome::ActionCacheHit, output));
            }
            // A failed result may come from a worker problem, such as running out of memory, so
            // it counts as a miss, and the execution replaces it.
            Some(_) => true,
            None => false,
        };
        if self.on_miss == ReapiOnMiss::Local {
            return compile_locally(out_pretty, CompileLocallyReason::NotInActionCache);
        }
        // The toolchain sent is the client's own, and REAPI workers run Linux.
        if !self.is_linux_client {
            return compile_locally(out_pretty, CompileLocallyReason::NonLinuxToolchain);
        }
        let Some(execution) = &apis.execution else {
            return compile_locally(out_pretty, CompileLocallyReason::ExecutionUnsupported);
        };
        if self.executions_refused.load(Ordering::Relaxed) {
            return compile_locally(out_pretty, CompileLocallyReason::ExecutionsRefused);
        }
        let rustc_check = self
            .rustc_checks
            .lock()
            .unwrap()
            .entry(reapi_compile.toolchain.clone())
            .or_default()
            .clone();
        // Without a call to make, the compile mustn't take the one a pause lets through.
        if let Some(RustcOnWorkers::Unusable(reason)) = rustc_check.outcome.get() {
            return compile_locally(out_pretty, *reason);
        }
        if !self.execution_pause.may_call() {
            return compile_locally(out_pretty, CompileLocallyReason::ExecutionsPaused);
        }
        let executed = async {
            self.check_rustc_on_workers(
                &rustc_check,
                &apis,
                execution,
                &reapi_compile.toolchain,
                &toolchain_files,
                out_pretty,
            )
            .await?;
            upload_and_execute(
                &apis,
                execution,
                &action,
                skip_cache_lookup,
                self.queue_timeout,
                out_pretty,
            )
            .await
        }
        .await;
        let response = match executed {
            Ok(response) => {
                self.execution_pause.call_succeeded();
                response
            }
            Err(CompileLocallyReason::ExecutionsRefused) => {
                if !self.executions_refused.swap(true, Ordering::Relaxed) {
                    warn!(
                        "REAPI: the server refused an execution; compiling misses locally until \
                         the sccache server restarts"
                    );
                }
                return Err(CompileLocallyReason::ExecutionsRefused);
            }
            // An input that can't be read or hashed says nothing about the server. The rustc
            // check already counted the server's answer, even if its stdout then failed to
            // download. Refused credentials make the connection check them again instead.
            Err(
                reason @ (CompileLocallyReason::UnreadableInput
                | CompileLocallyReason::CredentialsRefused
                | CompileLocallyReason::HashingInputsFailed
                | CompileLocallyReason::DownloadingOutputsFailed
                | CompileLocallyReason::RustcFailsOnWorkers
                | CompileLocallyReason::RustcCommitDiffersOnWorkers),
            ) => {
                return Err(reason);
            }
            Err(reason) => {
                if let Some(pause) = self.execution_pause.call_failed() {
                    warn!("REAPI: executions are failing; compiling misses locally for {pause:?}");
                }
                return Err(reason);
            }
        };
        let result = response.result.unwrap_or_default();
        // Compiling locally shows the error with local paths.
        if result.exit_code != 0 {
            return compile_locally(out_pretty, CompileLocallyReason::CompileFailedOnWorker);
        }
        let output = write_outputs(
            &apis,
            &reapi_compile,
            &result,
            &action.action_digest,
            out_pretty,
        )
        .await?;
        debug!(
            "[{}]: REAPI executed {} (cached: {})",
            out_pretty, action.action_digest.hash, response.cached_result
        );
        let outcome = if response.cached_result {
            ReapiOutcome::ActionCacheHit
        } else {
            ReapiOutcome::Executed
        };
        Ok((outcome, output))
    }

    /// Runs the `rustc -vV` of toolchain `key` on a worker, once per `key`. Fails when the
    /// workers can't run that rustc, or when it prints another commit hash than the local one.
    /// A check that fails to reach a worker isn't kept, so the next miss checks again, but
    /// compiles that waited for it compile locally.
    async fn check_rustc_on_workers(
        &self,
        check: &RustcCheck,
        apis: &ServerApis,
        execution: &Execution,
        key: &ToolchainKey,
        toolchain: &ToolchainFiles,
        out_pretty: &str,
    ) -> std::result::Result<(), CompileLocallyReason> {
        let failures_before = check.failures.lock().unwrap().0;
        let outcome = check
            .outcome
            .get_or_try_init(|| async {
                let (failures, last_failure) = *check.failures.lock().unwrap();
                if failures != failures_before
                    && let Some(reason) = last_failure
                {
                    return Err(reason);
                }
                let checked = self
                    .run_rustc_on_worker(apis, execution, key, toolchain, out_pretty)
                    .await;
                if let Err(reason) = checked {
                    *check.failures.lock().unwrap() = (failures + 1, Some(reason));
                }
                checked
            })
            .await?;
        match outcome {
            RustcOnWorkers::Unusable(reason) => compile_locally(out_pretty, *reason),
            RustcOnWorkers::Matches => Ok(()),
        }
    }

    async fn run_rustc_on_worker(
        &self,
        apis: &ServerApis,
        execution: &Execution,
        key: &ToolchainKey,
        toolchain: &ToolchainFiles,
        out_pretty: &str,
    ) -> std::result::Result<RustcOnWorkers, CompileLocallyReason> {
        let (files, action_options) = (toolchain.clone(), self.action_options.clone());
        let action = tokio::task::spawn_blocking(move || {
            rustc_version_action_blobs(&files, &action_options)
        })
        .await
        .map_err(Error::from)
        .flatten()
        .map_err(|e| {
            warn!(
                "[{}]: Compiling locally: failed to hash the toolchain: {:#}",
                out_pretty, e
            );
            CompileLocallyReason::HashingInputsFailed
        })?;
        let response = upload_and_execute(
            apis,
            execution,
            &action,
            false,
            self.queue_timeout,
            out_pretty,
        )
        .await?;
        self.execution_pause.call_succeeded();
        let result = response.result.unwrap_or_default();
        let sysroot = key.sysroot.display();
        if result.exit_code != 0 {
            warn!(
                "REAPI: rustc -vV of {sysroot} exits with {} on the workers; \
                 compiling its misses locally",
                result.exit_code
            );
            debug!(
                "REAPI: rustc -vV of {sysroot} printed on stderr: {}",
                loggable(&result.stderr_raw)
            );
            return Ok(RustcOnWorkers::Unusable(
                CompileLocallyReason::RustcFailsOnWorkers,
            ));
        }
        let stdout =
            download_stdout_or_stderr(&apis.cas, &result.stdout_raw, result.stdout_digest.as_ref())
                .await
                .map_err(|e| {
                    warn!("[{}]: Compiling locally: {:#}", out_pretty, e);
                    if is_refused_read(&e) {
                        CompileLocallyReason::CredentialsRefused
                    } else {
                        CompileLocallyReason::DownloadingOutputsFailed
                    }
                })?;
        let stdout = String::from_utf8_lossy(&stdout);
        let commit_hash = stdout
            .lines()
            .find_map(|line| line.strip_prefix("commit-hash: "));
        if commit_hash != Some(key.commit_hash.as_str()) {
            warn!(
                "REAPI: rustc {sysroot} reports commit hash {} on the workers and {} locally; \
                 compiling its misses locally",
                loggable(commit_hash.unwrap_or_default().as_bytes()),
                key.commit_hash
            );
            return Ok(RustcOnWorkers::Unusable(
                CompileLocallyReason::RustcCommitDiffersOnWorkers,
            ));
        }
        Ok(RustcOnWorkers::Matches)
    }
}

/// Worker output for a log line: escaped, and cut to 200 chars.
fn loggable(output: &[u8]) -> String {
    let output: String = String::from_utf8_lossy(output).chars().take(200).collect();
    format!("{output:?}")
}

/// Writes `result`'s outputs to `reapi_compile`'s local paths.
async fn write_outputs(
    apis: &ServerApis,
    reapi_compile: &ReapiCompile,
    result: &remexec::ActionResult,
    action_digest: &remexec::Digest,
    out_pretty: &str,
) -> std::result::Result<process::Output, CompileLocallyReason> {
    materialize(&apis.cas, reapi_compile, result)
        .await
        .map_err(|e| {
            let reason = if is_refused_read(&e) {
                CompileLocallyReason::CredentialsRefused
            } else {
                materialize_failure_reason(&e)
            };
            // The same result fails the same way on every build.
            let log_level = match reason {
                CompileLocallyReason::UnusableResult | CompileLocallyReason::WorkerRootInOutput => {
                    log::Level::Debug
                }
                _ => log::Level::Warn,
            };
            log!(
                log_level,
                "[{}]: Compiling locally: result of action {}: {:#}",
                out_pretty,
                action_digest.hash,
                e
            );
            reason
        })
}

/// Uploads what `action` needs and executes it, uploading again once if the CAS evicted
/// blobs since.
async fn upload_and_execute(
    apis: &ServerApis,
    execution: &Execution,
    action: &ActionBlobs,
    skip_cache_lookup: bool,
    queue_timeout: Duration,
    out_pretty: &str,
) -> std::result::Result<remexec::ExecuteResponse, CompileLocallyReason> {
    let deadline = tokio::time::Instant::now() + EXECUTE_TIMEOUT;
    let execute = || async {
        tokio::time::timeout_at(
            deadline,
            execution.execute_action(&action.action_digest, skip_cache_lookup, queue_timeout),
        )
        .await
        .unwrap_or_else(|_| bail!("the execution timed out after {EXECUTE_TIMEOUT:?}"))
    };
    upload(apis, action, out_pretty).await?;
    let executed = match execute().await {
        // The CAS may evict blobs after FindMissingBlobs, or after `Cas` last saw them.
        Err(e) if e.is::<MissingActionBlobs>() => {
            debug!("[{}]: {:#}; uploading again", out_pretty, e);
            apis.cas.forget_present_blobs();
            upload(apis, action, out_pretty).await?;
            execute().await
        }
        executed => executed,
    };
    executed.map_err(|e| {
        warn!("[{}]: Compiling locally: {:#}", out_pretty, e);
        if e.is::<QueuedTooLong>() {
            return CompileLocallyReason::ExecutionQueuedTooLong;
        }
        match rpc_code(&e) {
            Some(tonic::Code::Unauthenticated) => CompileLocallyReason::CredentialsRefused,
            // PERMISSION_DENIED on Execute comes from credentials that may look actions up but
            // not execute them.
            Some(tonic::Code::PermissionDenied) => CompileLocallyReason::ExecutionsRefused,
            // Including FAILED_PRECONDITION without missing blobs, which servers also answer
            // while no worker is available, as when a pool scales up from zero.
            _ => CompileLocallyReason::ExecutionFailed,
        }
    })
}

async fn upload(
    apis: &ServerApis,
    action: &ActionBlobs,
    out_pretty: &str,
) -> std::result::Result<(), CompileLocallyReason> {
    apis.cas
        .ensure_uploaded(action.blobs.clone())
        .await
        .map_err(|e| {
            warn!("[{}]: Compiling locally: {:#}", out_pretty, e);
            if e.is::<UnreadableBlob>() {
                return CompileLocallyReason::UnreadableInput;
            }
            match rpc_code(&e) {
                Some(tonic::Code::Unauthenticated) => CompileLocallyReason::CredentialsRefused,
                // PERMISSION_DENIED on an upload comes from credentials that may read but not
                // write.
                Some(tonic::Code::PermissionDenied) => CompileLocallyReason::ExecutionsRefused,
                _ => CompileLocallyReason::UploadFailed,
            }
        })
}

/// Whether `e` is a read the server refused. A server lets any credentials it accepts read,
/// so a refused read means refused credentials.
fn is_refused_read(e: &Error) -> bool {
    matches!(
        rpc_code(e),
        Some(tonic::Code::Unauthenticated | tonic::Code::PermissionDenied)
    )
}

/// Logs that `out_pretty` compiles locally for `reason`, and returns `reason` as the error.
fn compile_locally<T>(
    out_pretty: &str,
    reason: CompileLocallyReason,
) -> std::result::Result<T, CompileLocallyReason> {
    debug!("[{}]: Compiling locally: {}", out_pretty, reason);
    Err(reason)
}

/// Reads the auth token file and checks the options for connecting to `config`'s server,
/// without connecting.
fn read_connect_options(config: &ReapiConfig) -> Result<ConnectOptions> {
    let auth_token = config
        .auth_token_file
        .as_ref()
        .map(|file| {
            std::fs::read_to_string(file).with_context(|| {
                format!(
                    "failed to read the REAPI auth token from {}",
                    file.display()
                )
            })
        })
        .transpose()?;
    let options = ConnectOptions {
        endpoint: config.endpoint.clone(),
        instance_name: config.instance_name.clone(),
        auth_token,
    };
    check_connect_options(&options)?;
    Ok(options)
}

#[async_trait]
impl<T: CommandCreatorSync> RemoteExecutor<T> for ReapiExecutor {
    fn dist_input_options(&self) -> Option<DistInputOptions> {
        None
    }

    fn local_compile_changes(&self, compilation: &dyn Compilation<T>) -> LocalCompileChanges {
        compilation
            .reapi_local_compile_changes(&self.sysroot_copies_dir)
            .unwrap_or_default()
    }

    async fn compile(
        &self,
        job: RemoteJob<T>,
        local_compile: LocalCompile<'_, T>,
    ) -> Result<(DistType, process::Output)> {
        let RemoteJob {
            compilation,
            out_pretty,
            cache_control,
            local_compile_changed,
            ..
        } = job;
        let looked_up = async {
            let lookup = self
                .prepare_lookup(
                    compilation,
                    cache_control,
                    local_compile_changed,
                    &out_pretty,
                )
                .await?;
            let apis = lookup.apis.clone();
            let looked_up = self.look_up_or_execute(lookup, &out_pretty).await;
            if let (Err(CompileLocallyReason::CredentialsRefused), Some(connection)) =
                (&looked_up, &self.connection)
            {
                connection.credentials_refused(&apis).await;
            }
            looked_up
        }
        .await;
        match looked_up {
            Ok((outcome, output)) => Ok((DistType::Reapi(outcome), output)),
            // The stats count reasons by their text, as they do non-cacheable ones.
            Err(reason) => Ok((
                DistType::Reapi(ReapiOutcome::CompiledLocally(reason.to_string())),
                local_compile.run().await?,
            )),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::compiler::{
        CacheControl, Cacheable, CompileCommand, LocalArg, SingleCompileCommand,
    };
    use crate::dist;
    use crate::mock_command::{MockChild, MockCommandCreator, exit_status};
    use crate::reapi::fake::FakeServer;
    use crate::reapi::pause::FAILURES_TO_PAUSE;
    use crate::reapi::proto::build::bazel::remote::execution::v2 as remexec;
    use crate::reapi::remote_compile::test::{fake_sysroot, inline_output, worker_result};
    use crate::reapi::{PathMapper, ToolchainKey};
    use crate::server::SccacheService;
    use crate::test::utils::{new_creator, next_command};
    use std::ffi::OsString;
    use std::sync::Mutex;
    use std::sync::atomic::Ordering;

    type Creator = Arc<Mutex<MockCommandCreator>>;

    const TARGET_TRIPLE: &str = "x86_64-unknown-linux-gnu";
    const COMMIT_HASH: &str = "abc123";
    const RLIB: &str = "target/debug/deps/libfoo.rlib";

    #[derive(Clone, Copy)]
    enum Eligibility {
        Eligible,
        Ineligible,
        Panics,
    }

    /// A compile of `dir/ws/src/lib.rs` into `dir/ws/target/debug/deps/libfoo.rlib` with the
    /// toolchain at `dir/sysroot`. Its local compile gets `--sysroot=<sysroot_copies_dir>`.
    struct FakeCompilation {
        eligibility: Eligibility,
        dir: PathBuf,
        commit_hash: String,
    }

    impl Compilation<Creator> for FakeCompilation {
        fn generate_compile_commands(
            &self,
            _: &mut dist::PathTransformer,
            _: bool,
        ) -> Result<(
            Box<dyn CompileCommand<Creator>>,
            Option<dist::CompileCommand>,
            Cacheable,
        )> {
            unimplemented!()
        }

        fn map_for_reapi(&self) -> std::result::Result<ReapiCompile, crate::reapi::NotEligible> {
            match self.eligibility {
                Eligibility::Eligible => {}
                Eligibility::Ineligible => return Err(CompileLocallyReason::Staticlib.into()),
                Eligibility::Panics => panic!("map_for_reapi"),
            }
            let ws = self.dir.join("ws");
            Ok(ReapiCompile {
                path_mapper: PathMapper::new(ws.clone(), ws.join("target")),
                arguments: vec![],
                mapped_crate_root: None,
                toolchain: ToolchainKey::new(
                    self.commit_hash.clone(),
                    self.dir.join("sysroot"),
                    TARGET_TRIPLE.to_owned(),
                ),
                env: Default::default(),
                input_root_files: vec![ws.join("src/lib.rs")],
                outputs: vec![ws.join(RLIB)],
            })
        }

        fn reapi_local_compile_changes(
            &self,
            sysroot_copies_dir: &Path,
        ) -> Option<LocalCompileChanges> {
            let mut arg = OsString::from("--sysroot=");
            arg.push(sysroot_copies_dir);
            Some(LocalCompileChanges {
                args: vec![LocalArg {
                    hashed_as: arg.clone(),
                    arg,
                }],
                stdout_stderr_mapper: None,
            })
        }

        #[cfg(feature = "dist-client")]
        fn into_dist_packagers(
            self: Box<Self>,
            _: dist::PathTransformer,
        ) -> Result<crate::compiler::DistPackagers> {
            unimplemented!()
        }

        fn outputs<'a>(&'a self) -> Box<dyn Iterator<Item = crate::cache::FileObjectSource> + 'a> {
            Box::new(std::iter::empty())
        }
    }

    /// A config whose misses compile locally. The execution tests set `on_miss` to execute.
    fn config(endpoint: String, ready_wait_secs: u64) -> ReapiConfig {
        ReapiConfig {
            endpoint,
            ready_wait_secs,
            on_miss: ReapiOnMiss::Local,
            ..Default::default()
        }
    }

    /// An executor that waits up to 10s for a fake server, and a temp dir holding the
    /// toolchain and `ws/src/lib.rs`.
    struct Fixture {
        runtime: tokio::runtime::Runtime,
        server: Arc<FakeServer>,
        dir: tempfile::TempDir,
        config: ReapiConfig,
        eligibility: Eligibility,
        cache_control: CacheControl,
        local_compile_changed: bool,
        /// The local rustc's, which makes the toolchain key.
        commit_hash: String,
    }

    impl Fixture {
        fn new(eligibility: Eligibility) -> Self {
            Self::with_server(eligibility, FakeServer::default())
        }

        fn with_server(eligibility: Eligibility, server: FakeServer) -> Self {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let server = Arc::new(server);
            let config = config(runtime.block_on(server.serve()), 10);
            let dir = tempfile::tempdir().unwrap();
            fake_sysroot(dir.path());
            let lib_rs = dir.path().join("ws/src/lib.rs");
            std::fs::create_dir_all(lib_rs.parent().unwrap()).unwrap();
            std::fs::write(&lib_rs, "pub fn f() {}").unwrap();
            // Cargo creates the output directory before running rustc.
            std::fs::create_dir_all(dir.path().join("ws/target/debug/deps")).unwrap();
            Fixture {
                runtime,
                server,
                dir,
                config,
                eligibility,
                cache_control: CacheControl::Default,
                local_compile_changed: true,
                commit_hash: COMMIT_HASH.to_owned(),
            }
        }

        fn compilation(&self) -> FakeCompilation {
            FakeCompilation {
                eligibility: self.eligibility,
                dir: self.dir.path().to_owned(),
                commit_hash: self.commit_hash.clone(),
            }
        }

        /// Puts a successful result that writes an rlib holding `rlib`, changed by `change`, in
        /// the fake server's action cache.
        fn put_result_in_action_cache(&self, change: fn(&mut remexec::ActionResult)) {
            let compile = self.compilation().map_for_reapi().unwrap();
            let digest_cache = DigestCache::load(self.dir.path().join("digests.json"));
            let files = digest_input_root_files(&compile, &digest_cache).unwrap();
            let toolchain = crate::reapi::remote_compile::toolchain_files(
                &self.dir.path().join("sysroot"),
                TARGET_TRIPLE,
                &digest_cache,
            )
            .unwrap();
            let action_digest = action_blobs(
                &compile,
                &files,
                &toolchain,
                &ActionOptions::new(&self.config),
            )
            .unwrap()
            .action_digest;
            let rlib_path = compile
                .path_mapper
                .to_input_root(&compile.outputs[0])
                .unwrap();
            let mut result = worker_result(vec![inline_output(&rlib_path, b"rlib")]);
            change(&mut result);
            self.server.insert_action_result(&action_digest, result);
        }

        /// An executor on a Linux client.
        fn executor(&self) -> ReapiExecutor {
            let mut executor = ReapiExecutor::new(&self.config, &self.dir.path().join("dist"));
            executor.is_linux_client = true;
            executor
        }

        /// Runs the compile through a new executor. A local compile writes `out` to stdout and
        /// `err` to stderr.
        fn compile(&self) -> (DistType, process::Output) {
            self.compile_with(&self.executor())
        }

        /// Like `compile`, through `executor`.
        fn compile_with(&self, executor: &ReapiExecutor) -> (DistType, process::Output) {
            self.compile_at_once(executor, 1).pop().unwrap()
        }

        /// Runs `n` compiles through `executor` at once.
        fn compile_at_once(
            &self,
            executor: &ReapiExecutor,
            n: usize,
        ) -> Vec<(DistType, process::Output)> {
            let storage = Arc::new(crate::cache::disk::DiskCache::new(
                self.dir.path(),
                0,
                self.runtime.handle(),
                Default::default(),
                crate::cache::CacheMode::ReadWrite,
                vec![],
            ));
            let service = SccacheService::mock_with_storage(storage, self.runtime.handle().clone());
            let creators: Vec<_> = (0..n).map(|_| new_creator()).collect();
            let compiles = creators.iter().map(|creator| {
                next_command(creator, Ok(MockChild::new(exit_status(0), "out", "err")));
                let command = SingleCompileCommand {
                    executable: "rustc".into(),
                    arguments: vec![],
                    env_vars: vec![],
                    cwd: ".".into(),
                    share_jobserver: false,
                };
                let local_compile = LocalCompile {
                    service: &service,
                    creator,
                    command: crate::compiler::CCompileCommand::new(command),
                    stdout_stderr_mapper: None,
                };
                let job = RemoteJob {
                    compilation: Box::new(self.compilation()),
                    out_pretty: "lib.rs".to_owned(),
                    dist_job: None,
                    cache_control: self.cache_control,
                    local_compile_changed: self.local_compile_changed,
                };
                executor.compile(job, local_compile)
            });
            self.runtime
                .block_on(futures::future::join_all(compiles))
                .into_iter()
                .map(Result::unwrap)
                .collect()
        }
    }

    /// Compiles with the config changed by `configure`, which gets the fixture's temp dir.
    fn compile(
        eligibility: Eligibility,
        configure: impl FnOnce(&mut ReapiConfig, &Path),
    ) -> (DistType, process::Output) {
        let mut fixture = Fixture::new(eligibility);
        configure(&mut fixture.config, fixture.dir.path());
        fixture.compile()
    }

    fn compiled_locally(reason: &str) -> DistType {
        DistType::Reapi(ReapiOutcome::CompiledLocally(reason.to_owned()))
    }

    /// A fixture whose misses execute on `server`, whose workers run the toolchain's rustc and
    /// write the rlib.
    fn executing_fixture(server: FakeServer) -> Fixture {
        let mut fixture = Fixture::with_server(
            Eligibility::Eligible,
            FakeServer {
                executes_actions: true,
                ..server
            },
        );
        fixture.config.on_miss = ReapiOnMiss::Execute;
        let compile = fixture.compilation().map_for_reapi().unwrap();
        let rlib = compile
            .path_mapper
            .to_input_root(&compile.outputs[0])
            .unwrap();
        fixture
            .server
            .worker_outputs
            .lock()
            .unwrap()
            .insert(rlib, b"rlib".to_vec());
        *fixture.server.worker_rustc_commit_hash.lock().unwrap() = Some(COMMIT_HASH.to_owned());
        fixture
    }

    #[test]
    fn test_rustc_is_checked_on_a_worker_once() {
        let fixture = executing_fixture(FakeServer::default());
        let executor = fixture.executor();
        let (dist_type, _) = fixture.compile_with(&executor);
        assert_eq!(DistType::Reapi(ReapiOutcome::Executed), dist_type);
        fixture.server.action_results.lock().unwrap().clear();
        let (dist_type, _) = fixture.compile_with(&executor);
        assert_eq!(DistType::Reapi(ReapiOutcome::Executed), dist_type);
        // rustc -vV, then each compile.
        assert_eq!(3, fixture.server.execute_calls.load(Ordering::SeqCst));
    }

    #[test_case::test_case(None, "rustc -vV fails on the workers" ; "rustc fails")]
    #[test_case::test_case(
        Some("def456"),
        "rustc on the workers reports another commit hash" ;
        "another commit"
    )]
    fn test_a_toolchain_whose_rustc_check_fails_compiles_locally(
        worker_commit_hash: Option<&str>,
        reason: &str,
    ) {
        let fixture = executing_fixture(FakeServer::default());
        *fixture.server.worker_rustc_commit_hash.lock().unwrap() =
            worker_commit_hash.map(str::to_owned);
        let executor = fixture.executor();
        for _ in 0..2 {
            assert_eq!(compiled_locally(reason), fixture.compile_with(&executor).0);
        }
        // Only rustc -vV ran, once.
        assert_eq!(1, fixture.server.execute_calls.load(Ordering::SeqCst));
    }

    #[test]
    fn test_compiles_that_waited_for_a_failed_rustc_check_dont_check_again() {
        let fixture = executing_fixture(FakeServer {
            upload_error: Some(tonic::Code::Internal).into(),
            ..Default::default()
        });
        let executor = fixture.executor();
        for (dist_type, _) in fixture.compile_at_once(&executor, 2) {
            assert_eq!(
                compiled_locally("failed to upload the action's blobs"),
                dist_type
            );
        }
        assert_eq!(
            1,
            fixture
                .server
                .find_missing_blobs_calls
                .load(Ordering::SeqCst)
        );
    }

    /// Fails executions until they pause, then ends the pause.
    fn pause_executions(fixture: &Fixture, executor: &ReapiExecutor) {
        *fixture.server.upload_error.lock().unwrap() = Some(tonic::Code::Internal);
        for _ in 0..FAILURES_TO_PAUSE {
            fixture.compile_with(executor);
        }
        assert_eq!(
            compiled_locally("REAPI executions paused after failures"),
            fixture.compile_with(executor).0
        );
        *fixture.server.upload_error.lock().unwrap() = None;
        executor.execution_pause.end();
    }

    #[test]
    fn test_a_toolchain_known_to_fail_its_rustc_check_leaves_the_call_after_a_pause() {
        let mut fixture = executing_fixture(FakeServer::default());
        let executor = fixture.executor();
        fixture.commit_hash = "def456".to_owned();
        let rustc_differs = compiled_locally("rustc on the workers reports another commit hash");
        assert_eq!(rustc_differs, fixture.compile_with(&executor).0);
        fixture.commit_hash = COMMIT_HASH.to_owned();
        pause_executions(&fixture, &executor);
        fixture.commit_hash = "def456".to_owned();
        assert_eq!(rustc_differs, fixture.compile_with(&executor).0);
        fixture.commit_hash = COMMIT_HASH.to_owned();
        assert_eq!(
            DistType::Reapi(ReapiOutcome::Executed),
            fixture.compile_with(&executor).0
        );
    }

    #[test]
    fn test_a_rustc_check_the_workers_answer_ends_a_pause() {
        let mut fixture = executing_fixture(FakeServer::default());
        let executor = fixture.executor();
        pause_executions(&fixture, &executor);
        fixture.commit_hash = "def456".to_owned();
        assert_eq!(
            compiled_locally("rustc on the workers reports another commit hash"),
            fixture.compile_with(&executor).0
        );
        fixture.commit_hash = COMMIT_HASH.to_owned();
        assert_eq!(
            DistType::Reapi(ReapiOutcome::Executed),
            fixture.compile_with(&executor).0
        );
    }

    #[test]
    fn test_a_rustc_check_that_didnt_reach_a_worker_runs_again() {
        let fixture = executing_fixture(FakeServer {
            upload_error: Some(tonic::Code::Internal).into(),
            ..Default::default()
        });
        let executor = fixture.executor();
        assert_eq!(
            compiled_locally("failed to upload the action's blobs"),
            fixture.compile_with(&executor).0
        );
        *fixture.server.upload_error.lock().unwrap() = None;
        assert_eq!(
            DistType::Reapi(ReapiOutcome::Executed),
            fixture.compile_with(&executor).0
        );
        assert_eq!(2, fixture.server.execute_calls.load(Ordering::SeqCst));
    }

    #[test]
    fn test_a_miss_executes_and_writes_its_outputs() {
        let fixture = executing_fixture(FakeServer::default());
        let (dist_type, output) = fixture.compile();
        assert_eq!(DistType::Reapi(ReapiOutcome::Executed), dist_type);
        assert!(output.status.success());
        let rlib = fixture.dir.path().join("ws").join(RLIB);
        assert_eq!(b"rlib".as_slice(), std::fs::read(rlib).unwrap());
        // The execution cached its result.
        let (dist_type, _) = fixture.compile();
        assert_eq!(DistType::Reapi(ReapiOutcome::ActionCacheHit), dist_type);
        // rustc -vV, then the compile.
        assert_eq!(2, fixture.server.actions_run.load(Ordering::SeqCst));
    }

    #[test]
    fn test_a_miss_with_on_miss_local_writes_nothing_to_the_server() {
        let mut fixture = executing_fixture(FakeServer::default());
        fixture.config.on_miss = ReapiOnMiss::Local;
        let (dist_type, _) = fixture.compile();
        assert_eq!(compiled_locally("not in the action cache"), dist_type);
        assert_eq!(
            0,
            fixture
                .server
                .find_missing_blobs_calls
                .load(Ordering::SeqCst)
        );
        assert_eq!(0, fixture.server.execute_calls.load(Ordering::SeqCst));
    }

    #[test]
    fn test_a_miss_on_a_server_that_doesnt_execute_compiles_locally() {
        let mut fixture = Fixture::new(Eligibility::Eligible);
        fixture.config.on_miss = ReapiOnMiss::Execute;
        let (dist_type, _) = fixture.compile();
        assert_eq!(
            compiled_locally("REAPI server doesn't execute actions"),
            dist_type
        );
    }

    #[test]
    fn test_a_miss_on_another_os_compiles_locally() {
        let fixture = executing_fixture(FakeServer::default());
        let mut executor = fixture.executor();
        executor.is_linux_client = false;
        let (dist_type, _) = fixture.compile_with(&executor);
        assert_eq!(
            compiled_locally("toolchain that doesn't run on Linux workers"),
            dist_type
        );
        assert_eq!(0, fixture.server.execute_calls.load(Ordering::SeqCst));
    }

    #[test]
    fn test_a_compile_that_fails_on_a_worker_compiles_locally() {
        let fixture = executing_fixture(FakeServer {
            worker_exit_code: 1,
            ..Default::default()
        });
        let (dist_type, output) = fixture.compile();
        assert_eq!(compiled_locally("compile failed on a worker"), dist_type);
        assert_eq!(b"out".as_slice(), output.stdout);
        // rustc -vV and the compile, twice: the server cached neither.
        fixture.compile();
        assert_eq!(4, fixture.server.actions_run.load(Ordering::SeqCst));
    }

    #[test_case::test_case(
        FakeServer {
            upload_error: Some(tonic::Code::PermissionDenied).into(),
            ..Default::default()
        } ;
        "upload permission denied"
    )]
    #[test_case::test_case(
        FakeServer {
            blob_upload_error: Some(tonic::Code::PermissionDenied),
            ..Default::default()
        } ;
        "blob upload permission denied"
    )]
    #[test_case::test_case(
        FakeServer {
            execute_error: Some(tonic::Code::PermissionDenied),
            ..Default::default()
        } ;
        "execute permission denied"
    )]
    fn test_refused_executions_compile_misses_locally_and_keep_looking_up(server: FakeServer) {
        let fixture = executing_fixture(server);
        let executor = fixture.executor();
        assert_eq!(
            compiled_locally("REAPI server refused executions"),
            fixture.compile_with(&executor).0
        );
        let calls = |server: &FakeServer| {
            (
                server.find_missing_blobs_calls.load(Ordering::SeqCst),
                server.execute_calls.load(Ordering::SeqCst),
            )
        };
        let calls_before = calls(&fixture.server);
        for _ in 0..=FAILURES_TO_PAUSE {
            assert_eq!(
                compiled_locally("REAPI server refused executions"),
                fixture.compile_with(&executor).0
            );
        }
        assert_eq!(calls_before, calls(&fixture.server));
        fixture.put_result_in_action_cache(|_| {});
        assert_eq!(
            DistType::Reapi(ReapiOutcome::ActionCacheHit),
            fixture.compile_with(&executor).0
        );
    }

    /// Compiles twice, refused both times, then once more with the server answering and the
    /// result in the action cache.
    fn compile_after_refusing_twice(fixture: &Fixture) {
        let executor = fixture.executor();
        for _ in 0..2 {
            assert_eq!(
                compiled_locally("REAPI server refused the credentials"),
                fixture.compile_with(&executor).0
            );
        }
        *fixture.server.lookup_error.lock().unwrap() = None;
        *fixture.server.upload_error.lock().unwrap() = None;
        *fixture.server.read_error.lock().unwrap() = None;
        fixture.put_result_in_action_cache(|_| {});
        assert_eq!(
            compiled_locally("REAPI server refused the credentials"),
            fixture.compile_with(&executor).0
        );
        // The first refusal connected again, and the second ended the connection.
        assert_eq!(2, fixture.server.capabilities_calls.load(Ordering::SeqCst));
    }

    #[test_case::test_case(
        FakeServer {
            lookup_error: Some(tonic::Code::Unauthenticated).into(),
            ..Default::default()
        } ;
        "lookup unauthenticated"
    )]
    #[test_case::test_case(
        FakeServer {
            lookup_error: Some(tonic::Code::PermissionDenied).into(),
            ..Default::default()
        } ;
        "lookup permission denied"
    )]
    #[test_case::test_case(
        FakeServer {
            upload_error: Some(tonic::Code::Unauthenticated).into(),
            ..Default::default()
        } ;
        "upload unauthenticated"
    )]
    #[test_case::test_case(
        FakeServer {
            execute_error: Some(tonic::Code::Unauthenticated),
            ..Default::default()
        } ;
        "execute unauthenticated"
    )]
    #[test_case::test_case(
        FakeServer {
            read_error: Some(tonic::Code::PermissionDenied).into(),
            ..Default::default()
        } ;
        "rustc check download permission denied"
    )]
    fn test_credentials_refused_twice_compile_everything_locally(server: FakeServer) {
        compile_after_refusing_twice(&executing_fixture(server));
    }

    #[test]
    fn test_outputs_refused_twice_compile_everything_locally() {
        let fixture = Fixture::new(Eligibility::Eligible);
        // Without inline contents, the client downloads the rlib from the CAS.
        fixture.put_result_in_action_cache(|result| result.output_files[0].contents.clear());
        *fixture.server.read_error.lock().unwrap() = Some(tonic::Code::PermissionDenied);
        compile_after_refusing_twice(&fixture);
    }

    #[test]
    fn test_credentials_refused_once_are_checked_again() {
        let fixture = Fixture::new(Eligibility::Eligible);
        *fixture.server.lookup_error.lock().unwrap() = Some(tonic::Code::Unauthenticated);
        let executor = fixture.executor();
        assert_eq!(
            compiled_locally("REAPI server refused the credentials"),
            fixture.compile_with(&executor).0
        );
        *fixture.server.lookup_error.lock().unwrap() = None;
        fixture.put_result_in_action_cache(|_| {});
        assert_eq!(
            DistType::Reapi(ReapiOutcome::ActionCacheHit),
            fixture.compile_with(&executor).0
        );
    }

    #[test_case::test_case(
        FakeServer {
            fail_operations: true,
            ..Default::default()
        } ;
        "failed operation"
    )]
    #[test_case::test_case(
        FakeServer {
            execute_error: Some(tonic::Code::FailedPrecondition),
            ..Default::default()
        } ;
        "no worker available"
    )]
    fn test_misses_compile_locally_after_executions_fail(server: FakeServer) {
        let fixture = executing_fixture(server);
        let executor = fixture.executor();
        for _ in 0..FAILURES_TO_PAUSE {
            assert_eq!(
                compiled_locally("REAPI execution failed"),
                fixture.compile_with(&executor).0
            );
        }
        assert_eq!(
            compiled_locally("REAPI executions paused after failures"),
            fixture.compile_with(&executor).0
        );
        assert_eq!(
            FAILURES_TO_PAUSE as usize,
            fixture.server.execute_calls.load(Ordering::SeqCst)
        );
    }

    #[test]
    fn test_an_execution_queued_too_long_compiles_locally() {
        let fixture = executing_fixture(FakeServer {
            stages_then_stall: vec![remexec::execution_stage::Value::Queued],
            ..Default::default()
        });
        let mut executor = fixture.executor();
        executor.queue_timeout = Duration::from_millis(100);
        for _ in 0..2 {
            assert_eq!(
                compiled_locally("REAPI execution queued too long"),
                fixture.compile_with(&executor).0
            );
        }
        // A rustc check that stayed queued isn't kept, so the next miss checks again.
        assert_eq!(2, fixture.server.execute_calls.load(Ordering::SeqCst));
    }

    #[test]
    fn test_a_failed_upload_compiles_locally() {
        let fixture = executing_fixture(FakeServer {
            upload_error: Some(tonic::Code::Internal).into(),
            ..Default::default()
        });
        let (dist_type, _) = fixture.compile();
        assert_eq!(
            compiled_locally("failed to upload the action's blobs"),
            dist_type
        );
        assert_eq!(0, fixture.server.execute_calls.load(Ordering::SeqCst));
    }

    #[test]
    fn test_an_input_removed_after_hashing_compiles_locally() {
        let fixture = executing_fixture(FakeServer::default());
        *fixture.server.delete_at_find_missing.lock().unwrap() =
            Some(fixture.dir.path().join("ws/src/lib.rs"));
        let (dist_type, _) = fixture.compile();
        assert_eq!(
            compiled_locally("failed to read an input to upload it"),
            dist_type
        );
    }

    #[test]
    fn test_an_execution_answered_from_the_action_cache_counts_as_a_hit() {
        let fixture = executing_fixture(FakeServer {
            lookups_miss: true,
            ..Default::default()
        });
        fixture.put_result_in_action_cache(|_| {});
        let (dist_type, _) = fixture.compile();
        assert_eq!(DistType::Reapi(ReapiOutcome::ActionCacheHit), dist_type);
        assert_eq!(2, fixture.server.execute_calls.load(Ordering::SeqCst));
        // Only rustc -vV ran.
        assert_eq!(1, fixture.server.actions_run.load(Ordering::SeqCst));
    }

    #[test]
    fn test_blobs_the_cas_evicted_are_uploaded_again() {
        let fixture = executing_fixture(FakeServer {
            forget_blobs_at_next_execute: true.into(),
            ..Default::default()
        });
        let (dist_type, _) = fixture.compile();
        assert_eq!(DistType::Reapi(ReapiOutcome::Executed), dist_type);
        // rustc -vV twice, since the CAS forgot the blobs, then the compile.
        assert_eq!(3, fixture.server.execute_calls.load(Ordering::SeqCst));
    }

    #[test]
    fn test_a_failed_cached_result_executes_again() {
        let fixture = executing_fixture(FakeServer::default());
        fixture.put_result_in_action_cache(|result| result.exit_code = 1);
        let (dist_type, _) = fixture.compile();
        assert_eq!(DistType::Reapi(ReapiOutcome::Executed), dist_type);
        // rustc -vV, then the compile.
        assert_eq!(2, fixture.server.actions_run.load(Ordering::SeqCst));
    }

    #[test]
    fn test_an_ineligible_compile_runs_locally_and_counts_why() {
        let (dist_type, output) = compile(Eligibility::Ineligible, |_, _| {});
        assert_eq!(compiled_locally("staticlib crate type"), dist_type);
        assert_eq!(b"out".as_slice(), output.stdout);
    }

    #[test]
    fn test_a_hit_writes_its_outputs() {
        let fixture = Fixture::new(Eligibility::Eligible);
        fixture.put_result_in_action_cache(|_| {});
        let (dist_type, output) = fixture.compile();
        assert_eq!(DistType::Reapi(ReapiOutcome::ActionCacheHit), dist_type);
        assert!(output.status.success());
        let ws = fixture.dir.path().join("ws");
        assert_eq!(b"rlib".as_slice(), std::fs::read(ws.join(RLIB)).unwrap());
        let expected_stderr = format!("warning: in {}/src/lib.rs\n", ws.display());
        assert_eq!(expected_stderr.as_bytes(), output.stderr);
    }

    #[test]
    fn test_a_hit_needs_the_same_salt() {
        let mut fixture = Fixture::new(Eligibility::Eligible);
        fixture.config.salt = "1".to_owned();
        fixture.put_result_in_action_cache(|_| {});
        let (dist_type, _) = fixture.compile();
        assert_eq!(DistType::Reapi(ReapiOutcome::ActionCacheHit), dist_type);
        fixture.config.salt = "2".to_owned();
        let (dist_type, _) = fixture.compile();
        assert_eq!(compiled_locally("not in the action cache"), dist_type);
    }

    #[test]
    fn test_a_miss_compiles_locally() {
        let (dist_type, output) = compile(Eligibility::Eligible, |_, _| {});
        assert_eq!(compiled_locally("not in the action cache"), dist_type);
        assert_eq!(b"err".as_slice(), output.stderr);
    }

    #[test]
    fn test_a_failed_lookup_compiles_locally() {
        let fixture = Fixture::new(Eligibility::Eligible);
        *fixture.server.lookup_error.lock().unwrap() = Some(tonic::Code::Internal);
        let (dist_type, output) = fixture.compile();
        assert_eq!(compiled_locally("action cache lookup failed"), dist_type);
        assert_eq!(b"out".as_slice(), output.stdout);
    }

    #[test]
    fn test_compiles_skip_the_lookup_after_failures() {
        let fixture = Fixture::new(Eligibility::Eligible);
        *fixture.server.lookup_error.lock().unwrap() = Some(tonic::Code::Internal);
        let executor = fixture.executor();
        for _ in 0..FAILURES_TO_PAUSE {
            assert_eq!(
                compiled_locally("action cache lookup failed"),
                fixture.compile_with(&executor).0
            );
        }
        let (dist_type, output) = fixture.compile_with(&executor);
        assert_eq!(
            compiled_locally("action cache lookups paused after failures"),
            dist_type
        );
        assert_eq!(b"out".as_slice(), output.stdout);
    }

    #[test]
    fn test_failed_downloads_dont_pause_lookups() {
        let fixture = Fixture::new(Eligibility::Eligible);
        // Without inline contents, the client downloads the rlib from the CAS, which lacks it.
        fixture.put_result_in_action_cache(|result| result.output_files[0].contents.clear());
        let executor = fixture.executor();
        for _ in 0..=FAILURES_TO_PAUSE {
            assert_eq!(
                compiled_locally("failed to download the outputs"),
                fixture.compile_with(&executor).0
            );
        }
    }

    #[test_case::test_case(
        |result| result.exit_code = 1,
        "not in the action cache" ;
        "failed result"
    )]
    #[test_case::test_case(
        |result| result.stderr_raw.clear(),
        "unusable action cache result" ;
        "result without the worker root line"
    )]
    #[test_case::test_case(
        |result| {
            let path = result.output_files[0].path.clone();
            result.output_files[0] = inline_output(&path, b"built in /w/1");
        },
        "worker root in an output" ;
        "worker root in the rlib"
    )]
    #[test_case::test_case(
        // Without inline contents, the client downloads the rlib from the CAS, which lacks it.
        |result| result.output_files[0].contents.clear(),
        "failed to download the outputs" ;
        "rlib missing from the cas"
    )]
    fn test_a_result_that_cant_be_used_compiles_locally(
        change: fn(&mut remexec::ActionResult),
        reason: &str,
    ) {
        let fixture = Fixture::new(Eligibility::Eligible);
        fixture.put_result_in_action_cache(change);
        let (dist_type, output) = fixture.compile();
        assert_eq!(compiled_locally(reason), dist_type);
        assert_eq!(b"out".as_slice(), output.stdout);
    }

    #[test]
    fn test_a_hit_whose_outputs_cant_be_written_compiles_locally() {
        let fixture = Fixture::new(Eligibility::Eligible);
        fixture.put_result_in_action_cache(|_| {});
        std::fs::remove_dir_all(fixture.dir.path().join("ws/target")).unwrap();
        let (dist_type, _) = fixture.compile();
        assert_eq!(compiled_locally("failed to write the outputs"), dist_type);
    }

    #[test_case::test_case(CacheControl::ForceRecache ; "recache")]
    #[test_case::test_case(CacheControl::ForceNoCache ; "no cache")]
    fn test_a_compile_that_bypasses_the_cache_skips_the_lookup(cache_control: CacheControl) {
        let mut fixture = Fixture::new(Eligibility::Eligible);
        fixture.put_result_in_action_cache(|_| {});
        fixture.cache_control = cache_control;
        let (dist_type, _) = fixture.compile();
        assert_eq!(
            compiled_locally("SCCACHE_RECACHE or SCCACHE_NO_CACHE set"),
            dist_type
        );
    }

    #[test]
    fn test_an_unchanged_local_compile_skips_the_lookup() {
        let mut fixture = Fixture::new(Eligibility::Eligible);
        fixture.put_result_in_action_cache(|_| {});
        fixture.local_compile_changed = false;
        let (dist_type, _) = fixture.compile();
        assert_eq!(
            compiled_locally("local compile without the worker's arguments"),
            dist_type
        );
    }

    #[test]
    fn test_an_eligible_compile_with_a_toolchain_without_std_runs_locally() {
        let (dist_type, _) = compile(Eligibility::Eligible, |_, dir| {
            std::fs::remove_dir_all(dir.join("sysroot/lib/rustlib")).unwrap();
        });
        assert_eq!(
            compiled_locally("toolchain without rustc or std"),
            dist_type
        );
    }

    #[test]
    fn test_an_eligible_compile_that_doesnt_wait_finds_the_server_connecting() {
        let (dist_type, _) = compile(Eligibility::Eligible, |config, _| {
            config.ready_wait_secs = 0;
        });
        assert_eq!(
            compiled_locally("connecting to the REAPI server"),
            dist_type
        );
    }

    #[test_case::test_case(
        |config, _| config.endpoint = "farm:8980".to_owned() ;
        "bad endpoint"
    )]
    #[test_case::test_case(
        |config, dir| config.auth_token_file = Some(dir.join("missing")) ;
        "unreadable token file"
    )]
    #[test_case::test_case(
        |config, dir| {
            std::fs::write(dir.join("token"), " \n").unwrap();
            config.auth_token_file = Some(dir.join("token"));
        } ;
        "empty token file"
    )]
    fn test_a_misconfigured_executor_compiles_eligible_compiles_locally(
        configure: fn(&mut ReapiConfig, &Path),
    ) {
        let (dist_type, output) = compile(Eligibility::Eligible, configure);
        assert_eq!(compiled_locally("[dist.reapi] misconfigured"), dist_type);
        assert_eq!(b"out".as_slice(), output.stdout);
        let (dist_type, _) = compile(Eligibility::Ineligible, configure);
        assert_eq!(compiled_locally("staticlib crate type"), dist_type);
    }

    #[test]
    fn test_a_panicking_mapping_still_compiles_locally() {
        let (dist_type, output) = compile(Eligibility::Panics, |_, _| {});
        assert_eq!(
            compiled_locally("mapping the compile for REAPI panicked"),
            dist_type
        );
        assert_eq!(b"out".as_slice(), output.stdout);
    }

    #[test]
    fn test_local_compiles_get_the_sysroot_copies_dir_under_the_dist_cache_dir() {
        let executor =
            ReapiExecutor::new(&config("grpc://farm:8980".to_owned(), 0), "/cache".as_ref());
        let changes = RemoteExecutor::<Creator>::local_compile_changes(
            &executor,
            &FakeCompilation {
                eligibility: Eligibility::Eligible,
                dir: "/".into(),
                commit_hash: COMMIT_HASH.to_owned(),
            },
        );
        let mut expected = OsString::from("--sysroot=");
        expected.push(Path::new("/cache").join("reapi-sysroot-copies"));
        let args: Vec<_> = changes.args.into_iter().map(|a| a.arg).collect();
        assert_eq!(vec![expected], args);
    }
}
