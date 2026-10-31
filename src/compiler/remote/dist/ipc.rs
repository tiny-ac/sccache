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

//! sccache-dist in client-side mode, where the CLI packages a remote compile and the
//! daemon runs it with its dist client.

use super::{
    ClusterCompile, ClusterCompileOutput, DistError, DistExecutor, FailCompileReason,
    classify_error,
};
use crate::dist::{self, pkg};
use crate::errors::*;
use crate::server::DistClientContainer;
use fs_err as fs;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;

/// How long a compile waits for another CLI to send the toolchain it needs before
/// asking its own CLI for it.
const TOOLCHAIN_WAIT: Duration = Duration::from_secs(60);

/// Whether the daemon has a dist client, and its settings that affect cache keys.
#[derive(Debug, Serialize, Deserialize)]
pub enum DistClientSetup {
    Ready {
        rewrite_includes_only: bool,
        /// The custom toolchain archives in the daemon's config, by compiler executable.
        custom_toolchains: HashMap<PathBuf, PathBuf>,
    },
    /// No scheduler is configured, or the daemon is waiting to retry creating the client.
    NoDistClient,
    Misconfigured(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ToolchainPackaging {
    NotPackaged,
    Packaged { path: PathBuf },
    Failed(String),
}

/// A request for the daemon's dist client.
#[derive(Debug, Serialize, Deserialize)]
pub enum DistIpcRequest {
    GetDistClientSetup,
    CompileOnCluster {
        cluster_compile: Box<ClusterCompile>,
        inputs: InputsArchive,
        toolchain: ToolchainPackaging,
    },
}

/// The daemon's answer to a [`DistIpcRequest`].
#[derive(Debug, Serialize, Deserialize)]
pub enum DistIpcResponse {
    DistClientSetup(DistClientSetup),
    CompileOnCluster(CompileOnClusterResult),
}

#[derive(Serialize, Deserialize)]
pub enum CompileOnClusterResult {
    /// The daemon wrote the outputs to their local paths.
    RanOnBuildServer(ClusterCompileOutput),
    NoDistClient,
    /// The dist client has no archive of the compiler's toolchain; the CLI sends the
    /// request again with one.
    NeedToolchain,
    CompileLocally(CompileLocallyReason),
    FailCompile(FailCompileReason),
}

impl std::fmt::Debug for CompileOnClusterResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RanOnBuildServer(_) => f.write_str("RanOnBuildServer"),
            Self::NoDistClient => f.write_str("NoDistClient"),
            Self::NeedToolchain => f.write_str("NeedToolchain"),
            Self::CompileLocally(reason) => f.debug_tuple("CompileLocally").field(reason).finish(),
            Self::FailCompile(reason) => f.debug_tuple("FailCompile").field(reason).finish(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CompileLocallyReason {
    ClusterFailed(String),
    ToolchainPackagingFailed(String),
}

/// Error for a toolchain the CLI hasn't packaged.
#[derive(Debug)]
struct ToolchainNotPackaged;

impl std::fmt::Display for ToolchainNotPackaged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the CLI has not packaged the toolchain")
    }
}

impl std::error::Error for ToolchainNotPackaged {}

/// Toolchains the daemon has asked a CLI for, by weak toolchain key, so compiles that need
/// the same toolchain wait for one CLI to package it instead of each asking its own.
#[derive(Default)]
pub(crate) struct PendingDistToolchains {
    asks: Mutex<HashMap<String, ToolchainAsk>>,
}

struct ToolchainAsk {
    asked_at: Instant,
    /// Dropping it wakes the compiles waiting for the toolchain.
    wake_waiters: watch::Sender<()>,
}

enum AskOrWait {
    Ask,
    /// Wait for another CLI's toolchain until `deadline`.
    Wait {
        wake_up: watch::Receiver<()>,
        deadline: Instant,
    },
}

impl PendingDistToolchains {
    /// Whether a compile asks its CLI for `key`'s toolchain or waits for another CLI. A compile
    /// waits when another CLI was asked less than [`TOOLCHAIN_WAIT`] ago. `Ask` records the ask.
    fn ask_or_wait(&self, key: &str) -> AskOrWait {
        let mut asks = self.asks.lock().unwrap();
        match asks.get(key) {
            Some(ask) if ask.asked_at.elapsed() < TOOLCHAIN_WAIT => AskOrWait::Wait {
                wake_up: ask.wake_waiters.subscribe(),
                deadline: ask.asked_at + TOOLCHAIN_WAIT,
            },
            _ => {
                let ask = ToolchainAsk {
                    asked_at: Instant::now(),
                    wake_waiters: watch::channel(()).0,
                };
                asks.insert(key.to_owned(), ask);
                AskOrWait::Ask
            }
        }
    }

    /// Wakes the compiles waiting for `key`'s toolchain; the next compile without it asks
    /// its own CLI.
    fn wake_waiters(&self, key: &str) {
        self.asks.lock().unwrap().remove(key);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputsArchive {
    path: PathBuf,
    path_transformer: dist::PathTransformer,
}

impl pkg::InputsPackager for InputsArchive {
    fn write_inputs(self: Box<Self>, wtr: &mut dyn io::Write) -> Result<dist::PathTransformer> {
        io::copy(&mut fs::File::open(&self.path)?, wtr)?;
        Ok(self.path_transformer)
    }
}

/// Packages the toolchain by copying the archive the CLI wrote, if the CLI packaged one.
struct FileToolchainPackager {
    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    path: Option<PathBuf>,
    _wake_waiters_on_drop: Option<WakeWaitersOnDrop>,
}

/// Wakes the compiles waiting for a toolchain when the dist client drops the packager.
/// They then wait on the dist client's toolchain cache lock, which the packaging compile
/// holds until it has cached the archive or failed to.
struct WakeWaitersOnDrop {
    pending_dist_toolchains: Arc<PendingDistToolchains>,
    key: String,
}

impl Drop for WakeWaitersOnDrop {
    fn drop(&mut self) {
        self.pending_dist_toolchains.wake_waiters(&self.key);
    }
}

// On other targets, `pkg`'s blanket impl refuses to package.
#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
impl pkg::ToolchainPackager for FileToolchainPackager {
    fn write_pkg(self: Box<Self>, mut f: fs::File) -> Result<()> {
        let path = self.path.ok_or(ToolchainNotPackaged)?;
        io::copy(&mut fs::File::open(path)?, &mut f)?;
        Ok(())
    }
}

/// Answers `request` with the daemon's dist client, and resets the dist client after an
/// [`HttpClientError`].
pub(crate) async fn handle_dist_ipc_request(
    dist_client: &DistClientContainer,
    pending_dist_toolchains: &Arc<PendingDistToolchains>,
    request: DistIpcRequest,
) -> DistIpcResponse {
    match request {
        DistIpcRequest::GetDistClientSetup => {
            DistIpcResponse::DistClientSetup(dist_client.client_setup().await)
        }
        DistIpcRequest::CompileOnCluster {
            cluster_compile,
            inputs,
            toolchain,
        } => {
            let result = match dist_client.get_client().await {
                Ok(client) => {
                    handle_compile_on_cluster(
                        client,
                        pending_dist_toolchains,
                        *cluster_compile,
                        inputs,
                        toolchain,
                    )
                    .await
                }
                Err(e) => CompileOnClusterResult::FailCompile(FailCompileReason::Misconfigured(
                    format!("{e:#}"),
                )),
            };
            if let CompileOnClusterResult::FailCompile(FailCompileReason::HttpClientError(_)) =
                result
            {
                dist_client.reset_state().await;
            }
            DistIpcResponse::CompileOnCluster(result)
        }
    }
}

async fn handle_compile_on_cluster(
    dist_client: Option<Arc<dyn dist::Client>>,
    pending_dist_toolchains: &Arc<PendingDistToolchains>,
    cluster_compile: ClusterCompile,
    inputs: InputsArchive,
    toolchain: ToolchainPackaging,
) -> CompileOnClusterResult {
    let Some(dist_client) = dist_client else {
        return CompileOnClusterResult::NoDistClient;
    };
    let key = cluster_compile.weak_toolchain_key.clone();
    let out_pretty = cluster_compile.out_pretty.clone();
    let packaged_toolchain = match toolchain {
        ToolchainPackaging::NotPackaged => None,
        ToolchainPackaging::Packaged { path } => Some(path),
        ToolchainPackaging::Failed(e) => {
            pending_dist_toolchains.wake_waiters(&key);
            warn!("[{}]: Could not package toolchain: {}", out_pretty, e);
            return CompileOnClusterResult::CompileLocally(
                CompileLocallyReason::ToolchainPackagingFailed(e),
            );
        }
    };
    let executor = DistExecutor::new(dist_client);
    let result = loop {
        let inputs_packager = Box::new(inputs.clone());
        let toolchain_packager = Box::new(FileToolchainPackager {
            #[cfg(all(
                target_os = "linux",
                any(target_arch = "x86_64", target_arch = "aarch64")
            ))]
            path: packaged_toolchain.clone(),
            _wake_waiters_on_drop: packaged_toolchain.is_some().then(|| WakeWaitersOnDrop {
                pending_dist_toolchains: pending_dist_toolchains.clone(),
                key: key.clone(),
            }),
        });
        match executor
            .compile_on_cluster(cluster_compile.clone(), inputs_packager, toolchain_packager)
            .await
        {
            Err(e) if e.is::<ToolchainNotPackaged>() => {
                match pending_dist_toolchains.ask_or_wait(&key) {
                    AskOrWait::Wait {
                        mut wake_up,
                        deadline,
                    } => {
                        debug!("[{}]: Waiting for another CLI's toolchain", out_pretty);
                        // Woken when the CLI that was asked for the toolchain sends it or fails
                        // to package it, or at the deadline.
                        drop(tokio::time::timeout_at(deadline, wake_up.changed()).await);
                    }
                    AskOrWait::Ask => return CompileOnClusterResult::NeedToolchain,
                }
            }
            result => break result,
        }
    };
    match result {
        Ok(response) => CompileOnClusterResult::RanOnBuildServer(response),
        Err(e) => {
            let chain = format!("{e:#}");
            match classify_error(e, &cluster_compile.compiler_path) {
                DistError::CompileLocally(_) => {
                    warn!(
                        "[{}]: Could not perform distributed compile, compiling locally: {}",
                        out_pretty, chain
                    );
                    CompileOnClusterResult::CompileLocally(CompileLocallyReason::ClusterFailed(
                        chain,
                    ))
                }
                DistError::FailCompile(reason) => {
                    error!("[{}]: Distributed compile failed: {}", out_pretty, chain);
                    CompileOnClusterResult::FailCompile(reason)
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::dist::{
        AllocJobResult, JobAlloc, RunJobResult, SchedulerStatusResult, SubmitToolchainResult,
        Toolchain,
    };
    use async_trait::async_trait;
    use std::path::Path;

    /// A compile of `/usr/bin/cc`, with an inputs archive the CLI wrote into `dir`.
    fn compile_in(dir: &Path) -> (ClusterCompile, InputsArchive) {
        let path = dir.join("inputs.tar");
        std::fs::write(&path, b"inputs").unwrap();
        let cluster_compile = ClusterCompile {
            command: dist::CompileCommand {
                executable: "/usr/bin/cc".to_owned(),
                arguments: vec![],
                env_vars: vec![],
                cwd: "/".to_owned(),
            },
            dist_output_paths: vec![],
            compiler_path: "/usr/bin/cc".into(),
            weak_toolchain_key: "key".to_owned(),
            out_pretty: "foo.o".to_owned(),
        };
        let inputs = InputsArchive {
            path,
            path_transformer: dist::PathTransformer::new(),
        };
        (cluster_compile, inputs)
    }

    /// Handles a compile whose inputs archive the CLI wrote into `dir`.
    async fn handle_compile_on_cluster_in(
        dist_client: Option<Arc<dyn dist::Client>>,
        pending_dist_toolchains: &Arc<PendingDistToolchains>,
        dir: &Path,
        toolchain: ToolchainPackaging,
    ) -> CompileOnClusterResult {
        let (cluster_compile, inputs) = compile_in(dir);
        handle_compile_on_cluster(
            dist_client,
            pending_dist_toolchains,
            cluster_compile,
            inputs,
            toolchain,
        )
        .await
    }

    /// A dist client whose scheduler refuses its credentials.
    struct UnauthorizedClient;

    #[async_trait]
    impl dist::Client for UnauthorizedClient {
        async fn do_alloc_job(&self, _: Toolchain) -> Result<AllocJobResult> {
            Err(HttpClientError("MOCK: unauthorized".to_owned()).into())
        }
        async fn do_get_status(&self) -> Result<SchedulerStatusResult> {
            unreachable!()
        }
        async fn do_submit_toolchain(
            &self,
            _: JobAlloc,
            _: Toolchain,
        ) -> Result<SubmitToolchainResult> {
            unreachable!()
        }
        async fn do_run_job(
            &self,
            _: JobAlloc,
            _: dist::CompileCommand,
            _: Vec<String>,
            _: Box<dyn pkg::InputsPackager>,
        ) -> Result<(RunJobResult, dist::PathTransformer)> {
            unreachable!()
        }
        async fn put_toolchain(
            &self,
            _: PathBuf,
            _: String,
            _: Box<dyn pkg::ToolchainPackager>,
        ) -> Result<(Toolchain, Option<(String, PathBuf)>)> {
            let toolchain = Toolchain {
                archive_id: "somearchiveid".to_owned(),
            };
            Ok((toolchain, None))
        }
        fn rewrite_includes_only(&self) -> bool {
            false
        }
        fn get_custom_toolchain(&self, _: &Path) -> Option<PathBuf> {
            None
        }
    }

    /// An [`HttpClientError`] fails the compile and resets the daemon's dist client.
    #[tokio::test]
    async fn test_daemon_resets_its_dist_client_after_an_http_client_error() {
        let dir = tempfile::tempdir().unwrap();
        let (cluster_compile, inputs) = compile_in(dir.path());
        let dist_client = DistClientContainer::mock(
            Arc::new(UnauthorizedClient),
            tokio::runtime::Handle::current(),
        );
        let response = handle_dist_ipc_request(
            &dist_client,
            &Arc::default(),
            DistIpcRequest::CompileOnCluster {
                cluster_compile: Box::new(cluster_compile),
                inputs,
                toolchain: ToolchainPackaging::NotPackaged,
            },
        )
        .await;

        assert!(
            matches!(
                response,
                DistIpcResponse::CompileOnCluster(CompileOnClusterResult::FailCompile(
                    FailCompileReason::HttpClientError(_)
                ))
            ),
            "{response:?}"
        );
        // The mock config names no scheduler, so the reset dist client isn't created again.
        let setup = dist_client.client_setup().await;
        assert!(matches!(setup, DistClientSetup::NoDistClient), "{setup:?}");
    }

    #[tokio::test]
    async fn test_daemon_without_a_dist_client_answers_no_dist_client() {
        let dir = tempfile::tempdir().unwrap();
        let result = handle_compile_on_cluster_in(
            None,
            &Arc::default(),
            dir.path(),
            ToolchainPackaging::NotPackaged,
        )
        .await;
        assert!(
            matches!(result, CompileOnClusterResult::NoDistClient),
            "{result:?}"
        );
    }

    #[test]
    fn test_inputs_archive_copies_the_cli_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("inputs.tar");
        std::fs::write(&path, b"inputs").unwrap();
        let packager = Box::new(InputsArchive {
            path,
            path_transformer: dist::PathTransformer::new(),
        });
        let mut written = vec![];
        pkg::InputsPackager::write_inputs(packager, &mut written).unwrap();
        assert_eq!(b"inputs".as_slice(), written);
    }

    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    mod toolchain {
        use super::*;
        use std::sync::atomic::{AtomicUsize, Ordering};

        /// Caches the toolchain like the dist client does, then fails, as if the
        /// scheduler were down.
        #[derive(Default)]
        struct PackagingClient {
            cached_toolchain: Mutex<Option<Vec<u8>>>,
            times_packaged: AtomicUsize,
        }

        #[async_trait]
        impl dist::Client for PackagingClient {
            async fn do_alloc_job(&self, _: Toolchain) -> Result<AllocJobResult> {
                unreachable!()
            }
            async fn do_get_status(&self) -> Result<SchedulerStatusResult> {
                unreachable!()
            }
            async fn do_submit_toolchain(
                &self,
                _: JobAlloc,
                _: Toolchain,
            ) -> Result<SubmitToolchainResult> {
                unreachable!()
            }
            async fn do_run_job(
                &self,
                _: JobAlloc,
                _: dist::CompileCommand,
                _: Vec<String>,
                _: Box<dyn pkg::InputsPackager>,
            ) -> Result<(RunJobResult, dist::PathTransformer)> {
                unreachable!()
            }
            async fn put_toolchain(
                &self,
                _: PathBuf,
                _: String,
                toolchain_packager: Box<dyn pkg::ToolchainPackager>,
            ) -> Result<(Toolchain, Option<(String, PathBuf)>)> {
                let mut cached_toolchain = self.cached_toolchain.lock().unwrap();
                if cached_toolchain.is_none() {
                    let dir = tempfile::tempdir()?;
                    let path = dir.path().join("toolchain.tar");
                    toolchain_packager.write_pkg(fs::File::create(&path)?)?;
                    *cached_toolchain = Some(fs::read(&path)?);
                    self.times_packaged.fetch_add(1, Ordering::SeqCst);
                }
                bail!("the scheduler is down")
            }
            fn rewrite_includes_only(&self) -> bool {
                false
            }
            fn get_custom_toolchain(&self, _: &Path) -> Option<PathBuf> {
                None
            }
        }

        fn assert_cluster_failed(result: CompileOnClusterResult) {
            match result {
                CompileOnClusterResult::CompileLocally(CompileLocallyReason::ClusterFailed(
                    message,
                )) => {
                    assert!(message.contains("the scheduler is down"), "{message}");
                }
                other => panic!("expected ClusterFailed, got {other:?}"),
            }
        }

        #[tokio::test(start_paused = true)]
        async fn test_daemon_asks_for_the_toolchain_it_has_to_package() {
            let dir = tempfile::tempdir().unwrap();
            let client = Arc::new(PackagingClient::default());
            let result = handle_compile_on_cluster_in(
                Some(client.clone()),
                &Arc::default(),
                dir.path(),
                ToolchainPackaging::NotPackaged,
            )
            .await;
            assert!(
                matches!(result, CompileOnClusterResult::NeedToolchain),
                "{result:?}"
            );
            assert_eq!(0, client.times_packaged.load(Ordering::SeqCst));
        }

        #[tokio::test(start_paused = true)]
        async fn test_daemon_packages_the_toolchain_the_cli_sent() {
            let dir = tempfile::tempdir().unwrap();
            let toolchain = dir.path().join("toolchain.tar");
            std::fs::write(&toolchain, b"toolchain").unwrap();
            let client = Arc::new(PackagingClient::default());
            let result = handle_compile_on_cluster_in(
                Some(client.clone()),
                &Arc::default(),
                dir.path(),
                ToolchainPackaging::Packaged { path: toolchain },
            )
            .await;
            assert_cluster_failed(result);
            assert_eq!(
                client.cached_toolchain.lock().unwrap().as_deref(),
                Some(&b"toolchain"[..])
            );
        }

        async fn wait_until_a_compile_waits<T>(
            pending_dist_toolchains: &PendingDistToolchains,
            key: &str,
            compile: &tokio::task::JoinHandle<T>,
        ) {
            while pending_dist_toolchains.asks.lock().unwrap()[key]
                .wake_waiters
                .receiver_count()
                == 0
            {
                assert!(!compile.is_finished(), "the compile didn't wait");
                tokio::task::yield_now().await;
            }
        }

        /// When several compiles need the same toolchain, only the first asks its CLI for it.
        #[tokio::test(start_paused = true)]
        async fn test_daemon_asks_one_cli_for_a_toolchain() {
            let dir = tempfile::tempdir().unwrap();
            let client: Arc<PackagingClient> = Arc::default();
            let pending_dist_toolchains: Arc<PendingDistToolchains> = Arc::default();

            let first = handle_compile_on_cluster_in(
                Some(client.clone()),
                &pending_dist_toolchains,
                dir.path(),
                ToolchainPackaging::NotPackaged,
            )
            .await;
            assert!(
                matches!(first, CompileOnClusterResult::NeedToolchain),
                "{first:?}"
            );
            let second = tokio::spawn({
                let (client, pending_dist_toolchains) =
                    (client.clone(), pending_dist_toolchains.clone());
                let dir = dir.path().to_owned();
                async move {
                    handle_compile_on_cluster_in(
                        Some(client),
                        &pending_dist_toolchains,
                        &dir,
                        ToolchainPackaging::NotPackaged,
                    )
                    .await
                }
            });
            wait_until_a_compile_waits(&pending_dist_toolchains, "key", &second).await;
            let toolchain = dir.path().join("toolchain.tar");
            std::fs::write(&toolchain, b"toolchain").unwrap();
            let first_again = handle_compile_on_cluster_in(
                Some(client.clone()),
                &pending_dist_toolchains,
                dir.path(),
                ToolchainPackaging::Packaged { path: toolchain },
            )
            .await;

            assert_cluster_failed(first_again);
            // Shorter than TOOLCHAIN_WAIT, so a wake-up passes and a deadline doesn't.
            let second = tokio::time::timeout(Duration::from_secs(5), second)
                .await
                .expect("the waiting compile was not woken")
                .unwrap();
            assert_cluster_failed(second);
            assert_eq!(1, client.times_packaged.load(Ordering::SeqCst));
        }

        /// A packaging error from the CLI wakes the compiles waiting for the toolchain, which
        /// then ask their own CLIs.
        #[tokio::test(start_paused = true)]
        async fn test_daemon_wakes_waiting_compiles_when_packaging_fails() {
            let dir = tempfile::tempdir().unwrap();
            let client: Arc<PackagingClient> = Arc::default();
            let pending_dist_toolchains: Arc<PendingDistToolchains> = Arc::default();

            let first = handle_compile_on_cluster_in(
                Some(client.clone()),
                &pending_dist_toolchains,
                dir.path(),
                ToolchainPackaging::NotPackaged,
            )
            .await;
            assert!(
                matches!(first, CompileOnClusterResult::NeedToolchain),
                "{first:?}"
            );
            let second = tokio::spawn({
                let (client, pending_dist_toolchains) =
                    (client.clone(), pending_dist_toolchains.clone());
                let dir = dir.path().to_owned();
                async move {
                    handle_compile_on_cluster_in(
                        Some(client),
                        &pending_dist_toolchains,
                        &dir,
                        ToolchainPackaging::NotPackaged,
                    )
                    .await
                }
            });
            wait_until_a_compile_waits(&pending_dist_toolchains, "key", &second).await;
            let first_again = handle_compile_on_cluster_in(
                Some(client.clone()),
                &pending_dist_toolchains,
                dir.path(),
                ToolchainPackaging::Failed("no ldd".to_owned()),
            )
            .await;

            match first_again {
                CompileOnClusterResult::CompileLocally(
                    CompileLocallyReason::ToolchainPackagingFailed(message),
                ) => {
                    assert!(message.contains("no ldd"), "{message}");
                }
                other => panic!("expected ToolchainPackagingFailed, got {other:?}"),
            }
            let second = tokio::time::timeout(Duration::from_secs(5), second)
                .await
                .expect("the waiting compile was not woken")
                .unwrap();
            assert!(
                matches!(second, CompileOnClusterResult::NeedToolchain),
                "{second:?}"
            );
        }

        /// A compile waiting for a toolchain that never arrives asks its own CLI after
        /// [`TOOLCHAIN_WAIT`].
        #[tokio::test(start_paused = true)]
        async fn test_a_waiting_compile_asks_its_own_cli_after_the_toolchain_wait() {
            let dir = tempfile::tempdir().unwrap();
            let client: Arc<PackagingClient> = Arc::default();
            let pending_dist_toolchains: Arc<PendingDistToolchains> = Arc::default();

            let first = handle_compile_on_cluster_in(
                Some(client.clone()),
                &pending_dist_toolchains,
                dir.path(),
                ToolchainPackaging::NotPackaged,
            )
            .await;
            assert!(
                matches!(first, CompileOnClusterResult::NeedToolchain),
                "{first:?}"
            );
            let start = Instant::now();
            let second = tokio::time::timeout(
                TOOLCHAIN_WAIT * 2,
                handle_compile_on_cluster_in(
                    Some(client.clone()),
                    &pending_dist_toolchains,
                    dir.path(),
                    ToolchainPackaging::NotPackaged,
                ),
            )
            .await
            .expect("the waiting compile never asked its CLI");
            assert!(
                matches!(second, CompileOnClusterResult::NeedToolchain),
                "{second:?}"
            );
            assert!(start.elapsed() >= TOOLCHAIN_WAIT, "{:?}", start.elapsed());
        }
    }
}
