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

//! REAPI in client-side mode, where the CLI maps a cache miss for REAPI and the daemon's
//! executor compiles it through REAPI, or holds a local slot while the CLI compiles it.

use super::executor::{
    CompileLocally, ReapiExecutor, SYSROOT_COPIES_DIR, compiled_locally, map_miss,
};
use super::mapping::{CompileLocallyReason, ReapiCompile};
use crate::client::ServerConnection;
use crate::compiler::{
    Compilation, DistInputOptions, DistType, LocalCompile, LocalCompileChanges, ReapiOutcome,
    RemoteExecutor, RemoteJob,
};
use crate::dist::ProcessOutput;
use crate::errors::*;
use crate::mock_command::CommandCreatorSync;
use crate::protocol::{Request, Response};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::{Arc, Mutex};
use tokio::sync::{OnceCell, OwnedSemaphorePermit};

/// A request for the daemon's REAPI executor.
#[derive(Debug, Serialize, Deserialize)]
pub enum ReapiIpcRequest {
    GetReapiExecutorSetup,
    CompileThroughReapi {
        /// The miss mapped for REAPI, or why it compiles locally.
        mapped: std::result::Result<Box<ReapiCompile>, CompileLocallyReason>,
        out_pretty: String,
    },
    /// Sent when the CLI's local compile ends.
    ReleaseLocalSlot,
}

/// The daemon's answer to a [`ReapiIpcRequest`].
#[derive(Debug, Serialize, Deserialize)]
pub enum ReapiIpcResponse {
    ReapiExecutorSetup(ReapiExecutorSetup),
    CompileThroughReapi(CompileThroughReapiResult),
    LocalSlotReleased,
}

/// Whether the daemon has a REAPI executor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReapiExecutorSetup {
    Ready,
    NoReapiExecutor,
}

#[derive(Serialize, Deserialize)]
pub enum CompileThroughReapiResult {
    /// The daemon wrote the outputs to their local paths.
    FromReapi {
        outcome: ReapiOutcome,
        output: ProcessOutput,
    },
    NoReapiExecutor,
    /// The daemon holds a local slot until the CLI sends
    /// [`ReapiIpcRequest::ReleaseLocalSlot`].
    CompileLocally(CompileLocallyReason),
}

impl std::fmt::Debug for CompileThroughReapiResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FromReapi { outcome, .. } => f.debug_tuple("FromReapi").field(outcome).finish(),
            Self::NoReapiExecutor => f.write_str("NoReapiExecutor"),
            Self::CompileLocally(reason) => f.debug_tuple("CompileLocally").field(reason).finish(),
        }
    }
}

/// Answers `request` with the daemon's REAPI executor. `local_slot` is the slot the daemon
/// holds for the local compile of the connection's CLI.
pub(crate) async fn handle_reapi_ipc_request(
    executor: Option<&ReapiExecutor>,
    local_slot: &mut Option<OwnedSemaphorePermit>,
    request: ReapiIpcRequest,
) -> ReapiIpcResponse {
    match request {
        ReapiIpcRequest::GetReapiExecutorSetup => {
            ReapiIpcResponse::ReapiExecutorSetup(match executor {
                Some(_) => ReapiExecutorSetup::Ready,
                None => ReapiExecutorSetup::NoReapiExecutor,
            })
        }
        ReapiIpcRequest::CompileThroughReapi { mapped, out_pretty } => {
            // A CLI that sends another compile has ended the last one.
            *local_slot = None;
            let Some(executor) = executor else {
                return ReapiIpcResponse::CompileThroughReapi(
                    CompileThroughReapiResult::NoReapiExecutor,
                );
            };
            let mapped = mapped.map(|reapi_compile| *reapi_compile);
            let compiled = executor.compile_through_reapi(mapped, &out_pretty).await;
            ReapiIpcResponse::CompileThroughReapi(
                compile_through_reapi_result(executor, compiled, local_slot, &out_pretty).await,
            )
        }
        ReapiIpcRequest::ReleaseLocalSlot => {
            *local_slot = None;
            ReapiIpcResponse::LocalSlotReleased
        }
    }
}

/// The daemon's answer for `compiled`. A compile that runs locally puts its local slot in
/// `local_slot`.
async fn compile_through_reapi_result(
    executor: &ReapiExecutor,
    compiled: std::result::Result<(ReapiOutcome, process::Output), CompileLocally>,
    local_slot: &mut Option<OwnedSemaphorePermit>,
    out_pretty: &str,
) -> CompileThroughReapiResult {
    match compiled {
        Ok((outcome, output)) => match output.status.code() {
            Some(code) => CompileThroughReapiResult::FromReapi {
                outcome,
                output: ProcessOutput {
                    code,
                    stdout: output.stdout,
                    stderr: output.stderr,
                },
            },
            // `materialize` builds the status from the action's exit code, so an output without
            // one is a bug in sccache. The CLI then compiles locally, in a local slot like every
            // other local compile.
            None => {
                warn!("[{out_pretty}]: REAPI: the remote output has no exit code");
                *local_slot = executor.local_slot().await;
                CompileThroughReapiResult::CompileLocally(CompileLocallyReason::DaemonFailed)
            }
        },
        Err(CompileLocally {
            reason,
            local_slot: slot,
        }) => {
            *local_slot = slot;
            CompileThroughReapiResult::CompileLocally(reason)
        }
    }
}

/// A [`RemoteExecutor`] that compiles a REAPI build's misses through the daemon's REAPI
/// executor.
pub(crate) struct ReapiIpcExecutor {
    conn: Arc<Mutex<ServerConnection>>,
    /// Asked of the daemon on first use.
    setup: OnceCell<ReapiExecutorSetup>,
    sysroot_copies_dir: PathBuf,
}

impl ReapiIpcExecutor {
    pub(crate) fn new(conn: Arc<Mutex<ServerConnection>>, dist_cache_dir: &Path) -> Self {
        ReapiIpcExecutor {
            conn,
            setup: OnceCell::new(),
            sysroot_copies_dir: dist_cache_dir.join(SYSROOT_COPIES_DIR),
        }
    }

    /// False when the daemon can't answer.
    pub(crate) async fn has_reapi_executor(&self) -> bool {
        let setup = self
            .setup
            .get_or_try_init(|| async {
                match self.request(ReapiIpcRequest::GetReapiExecutorSetup).await? {
                    ReapiIpcResponse::ReapiExecutorSetup(setup) => Ok(setup),
                    other => bail!(
                        "unexpected response to ReapiIpcRequest::GetReapiExecutorSetup: {other:?}"
                    ),
                }
            })
            .await;
        match setup {
            Ok(setup) => *setup == ReapiExecutorSetup::Ready,
            Err(e) => {
                warn!("Could not ask the daemon for its REAPI executor, compiling locally: {e:#}");
                false
            }
        }
    }

    async fn request(&self, request: ReapiIpcRequest) -> Result<ReapiIpcResponse> {
        let conn = Arc::clone(&self.conn);
        let response = tokio::task::spawn_blocking(move || {
            conn.lock().unwrap().request(Request::Reapi(request))
        })
        .await
        .context("spawn_blocking panicked")??;
        match response {
            Response::Reapi(response) => Ok(response),
            other => bail!("unexpected response to a REAPI request: {other:?}"),
        }
    }
}

#[async_trait]
impl<T: CommandCreatorSync> RemoteExecutor<T> for ReapiIpcExecutor {
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
        let mapped = map_miss(
            compilation,
            cache_control,
            local_compile_changed,
            &out_pretty,
        )
        .await;
        let request = ReapiIpcRequest::CompileThroughReapi {
            mapped: mapped.map(Box::new),
            out_pretty: out_pretty.clone(),
        };
        let result = match self.request(request).await {
            Ok(ReapiIpcResponse::CompileThroughReapi(result)) => Ok(result),
            Ok(other) => Err(anyhow!(
                "unexpected response to ReapiIpcRequest::CompileThroughReapi: {other:?}"
            )),
            Err(e) => Err(e),
        };
        let reason = match result {
            Ok(CompileThroughReapiResult::FromReapi { outcome, output }) => {
                return Ok((DistType::Reapi(outcome), output.into()));
            }
            Ok(CompileThroughReapiResult::CompileLocally(reason)) => reason,
            Ok(CompileThroughReapiResult::NoReapiExecutor) => {
                debug!("[{out_pretty}]: The daemon has no REAPI executor, compiling locally");
                return Ok((DistType::NoDist, local_compile.run().await?));
            }
            // A daemon that didn't answer holds no local slot for the compile.
            Err(e) => {
                warn!("[{out_pretty}]: REAPI compile request failed, compiling locally: {e:#}");
                let reason = CompileLocallyReason::DaemonFailed;
                return Ok((compiled_locally(reason), local_compile.run().await?));
            }
        };
        let output = local_compile.run().await;
        if let Err(e) = self.request(ReapiIpcRequest::ReleaseLocalSlot).await {
            warn!("[{out_pretty}]: Could not release the daemon's local slot: {e:#}");
        }
        Ok((compiled_locally(reason), output?))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::reapi::executor::test::{Eligibility, Fixture, RLIB};
    use std::num::NonZeroUsize;
    use std::time::Duration;

    fn compile_request(fixture: &Fixture) -> ReapiIpcRequest {
        ReapiIpcRequest::CompileThroughReapi {
            mapped: (fixture.compilation().map_for_reapi())
                .map(Box::new)
                .map_err(|not_eligible| not_eligible.reason),
            out_pretty: "lib.rs".to_owned(),
        }
    }

    fn staticlib_request() -> ReapiIpcRequest {
        ReapiIpcRequest::CompileThroughReapi {
            mapped: Err(CompileLocallyReason::Staticlib),
            out_pretty: "lib.rs".to_owned(),
        }
    }

    /// A fixture whose executor has one local slot.
    fn one_local_slot(eligibility: Eligibility) -> Fixture {
        let mut fixture = Fixture::new(eligibility);
        fixture.config.local_slots = NonZeroUsize::new(1);
        fixture
    }

    #[tokio::test]
    async fn test_daemon_without_a_reapi_executor_answers_no_reapi_executor() {
        let setup =
            handle_reapi_ipc_request(None, &mut None, ReapiIpcRequest::GetReapiExecutorSetup).await;
        assert!(
            matches!(
                setup,
                ReapiIpcResponse::ReapiExecutorSetup(ReapiExecutorSetup::NoReapiExecutor)
            ),
            "{setup:?}"
        );
        let compiled = handle_reapi_ipc_request(None, &mut None, staticlib_request()).await;
        assert!(
            matches!(
                compiled,
                ReapiIpcResponse::CompileThroughReapi(CompileThroughReapiResult::NoReapiExecutor)
            ),
            "{compiled:?}"
        );
    }

    #[test]
    fn test_the_daemon_writes_the_outputs_of_a_hit() {
        let fixture = Fixture::new(Eligibility::Eligible);
        fixture.put_result_in_action_cache(|_| {});
        let executor = fixture.executor();
        let setup = fixture.runtime.block_on(handle_reapi_ipc_request(
            Some(&executor),
            &mut None,
            ReapiIpcRequest::GetReapiExecutorSetup,
        ));
        assert!(
            matches!(
                setup,
                ReapiIpcResponse::ReapiExecutorSetup(ReapiExecutorSetup::Ready)
            ),
            "{setup:?}"
        );
        let compiled = fixture.runtime.block_on(handle_reapi_ipc_request(
            Some(&executor),
            &mut None,
            compile_request(&fixture),
        ));
        match compiled {
            ReapiIpcResponse::CompileThroughReapi(CompileThroughReapiResult::FromReapi {
                outcome,
                output,
            }) => {
                assert_eq!(ReapiOutcome::ActionCacheHit, outcome);
                assert_eq!(0, output.code);
            }
            other => panic!("expected FromReapi, got {other:?}"),
        }
        let rlib = std::fs::read(fixture.dir.path().join("ws").join(RLIB)).unwrap();
        assert_eq!(b"rlib", rlib.as_slice());
    }

    #[test]
    fn test_daemon_holds_local_slot_until_release() {
        let fixture = one_local_slot(Eligibility::Eligible);
        let executor = fixture.executor();
        fixture.runtime.block_on(async {
            let mut first = None;
            let compiled =
                handle_reapi_ipc_request(Some(&executor), &mut first, staticlib_request()).await;
            assert!(
                matches!(
                    compiled,
                    ReapiIpcResponse::CompileThroughReapi(
                        CompileThroughReapiResult::CompileLocally(CompileLocallyReason::Staticlib)
                    )
                ),
                "{compiled:?}"
            );
            let mut second = None;
            let waited = tokio::time::timeout(
                Duration::from_millis(200),
                handle_reapi_ipc_request(Some(&executor), &mut second, staticlib_request()),
            )
            .await;
            assert!(waited.is_err(), "another CLI's compile took the held slot");
            handle_reapi_ipc_request(
                Some(&executor),
                &mut first,
                ReapiIpcRequest::ReleaseLocalSlot,
            )
            .await;
            tokio::time::timeout(
                Duration::from_secs(30),
                handle_reapi_ipc_request(Some(&executor), &mut second, staticlib_request()),
            )
            .await
            .expect("the released slot didn't free");
        });
    }

    #[test]
    fn test_a_compile_request_releases_the_slot_of_the_last_compile() {
        let fixture = one_local_slot(Eligibility::Eligible);
        let executor = fixture.executor();
        fixture.runtime.block_on(async {
            let mut local_slot = None;
            for _ in 0..2 {
                tokio::time::timeout(
                    Duration::from_secs(30),
                    handle_reapi_ipc_request(Some(&executor), &mut local_slot, staticlib_request()),
                )
                .await
                .expect("the compile waited for the connection's own slot");
            }
        });
    }

    #[cfg(unix)]
    #[test]
    fn test_an_output_without_an_exit_code_compiles_locally_in_a_local_slot() {
        use std::os::unix::process::ExitStatusExt;

        let fixture = one_local_slot(Eligibility::Eligible);
        let executor = fixture.executor();
        let killed = process::Output {
            status: process::ExitStatus::from_raw(9),
            stdout: vec![],
            stderr: vec![],
        };
        let mut local_slot = None;
        let result = fixture.runtime.block_on(compile_through_reapi_result(
            &executor,
            Ok((ReapiOutcome::ActionCacheHit, killed)),
            &mut local_slot,
            "lib.rs",
        ));
        assert!(
            matches!(
                result,
                CompileThroughReapiResult::CompileLocally(CompileLocallyReason::DaemonFailed)
            ),
            "{result:?}"
        );
        assert!(
            local_slot.is_some(),
            "the CLI's local compile has no local slot"
        );
    }

    /// A connection to `addr` whose reads fail after a minute, so that a daemon that stops
    /// answering fails the test instead of hanging it.
    fn connect(addr: &crate::net::SocketAddr) -> ServerConnection {
        let crate::net::SocketAddr::Net(addr) = addr else {
            unreachable!("the test daemons listen on TCP");
        };
        let stream = std::net::TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(60)))
            .unwrap();
        ServerConnection::new(Box::new(stream)).unwrap()
    }

    /// An sccache daemon with a REAPI executor, running until [`TestDaemon::stop`].
    struct TestDaemon {
        addr: crate::net::SocketAddr,
        shutdown: futures::channel::oneshot::Sender<()>,
        thread: std::thread::JoinHandle<()>,
    }

    impl TestDaemon {
        fn start(executor: ReapiExecutor, cache_dir: PathBuf) -> Self {
            use crate::server::DistClientContainer;

            let (addr_tx, addr_rx) = std::sync::mpsc::channel();
            let (shutdown, shutdown_rx) = futures::channel::oneshot::channel::<()>();
            let thread = std::thread::spawn(move || {
                let dist = |_: &tokio::runtime::Handle| {
                    DistClientContainer::new_disabled().with_reapi_executor(Arc::new(executor))
                };
                crate::test::utils::run_test_daemon(&cache_dir, dist, addr_tx, shutdown_rx);
            });
            TestDaemon {
                addr: addr_rx.recv().unwrap(),
                shutdown,
                thread,
            }
        }

        fn connect(&self) -> ServerConnection {
            connect(&self.addr)
        }

        /// Waits for the open connections to close first.
        fn stop(self) {
            self.shutdown.send(()).unwrap();
            self.thread.join().unwrap();
        }
    }

    fn send(conn: &mut ServerConnection, request: ReapiIpcRequest) -> ReapiIpcResponse {
        match conn.request(Request::Reapi(request)).unwrap() {
            Response::Reapi(response) => response,
            other => panic!("unexpected response: {other:?}"),
        }
    }

    /// Each connection has its own local slot, which the daemon releases when the connection
    /// closes.
    #[test]
    fn test_a_closed_connection_releases_its_local_slot() {
        let fixture = one_local_slot(Eligibility::Eligible);
        let daemon = TestDaemon::start(fixture.executor(), fixture.dir.path().join("daemon"));
        let mut first = daemon.connect();
        let compiled = send(&mut first, staticlib_request());
        assert!(
            matches!(
                compiled,
                ReapiIpcResponse::CompileThroughReapi(CompileThroughReapiResult::CompileLocally(_))
            ),
            "{compiled:?}"
        );
        let mut second = daemon.connect();
        let (compiled_tx, compiled_rx) = std::sync::mpsc::channel();
        let waiting = std::thread::spawn(move || {
            compiled_tx
                .send(send(&mut second, staticlib_request()))
                .unwrap();
            second
        });
        assert!(
            compiled_rx
                .recv_timeout(Duration::from_millis(200))
                .is_err(),
            "another CLI's compile took the held slot"
        );
        drop(first);
        let compiled = compiled_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("the closed connection kept its local slot");
        assert!(
            matches!(
                compiled,
                ReapiIpcResponse::CompileThroughReapi(CompileThroughReapiResult::CompileLocally(_))
            ),
            "{compiled:?}"
        );
        drop(waiting.join().unwrap());
        daemon.stop();
    }

    /// A CLI's executor connected to `addr`.
    fn cli_executor(fixture: &Fixture, addr: &crate::net::SocketAddr) -> ReapiIpcExecutor {
        let conn = connect(addr);
        let dist_cache_dir = fixture.dir.path().join("cli-dist");
        ReapiIpcExecutor::new(Arc::new(Mutex::new(conn)), &dist_cache_dir)
    }

    #[test]
    fn test_a_cli_gets_a_hit_through_the_daemon() {
        let fixture = Fixture::new(Eligibility::Eligible);
        fixture.put_result_in_action_cache(|_| {});
        let daemon = TestDaemon::start(fixture.executor(), fixture.dir.path().join("daemon"));
        let executor = cli_executor(&fixture, &daemon.addr);
        let (dist_type, output) = fixture.compile_with(&executor);
        assert_eq!(DistType::Reapi(ReapiOutcome::ActionCacheHit), dist_type);
        // The worker's stderr, which a local compile doesn't print.
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("warning: in"), "{stderr}");
        let rlib = std::fs::read(fixture.dir.path().join("ws").join(RLIB)).unwrap();
        assert_eq!(b"rlib", rlib.as_slice());
        drop(executor);
        daemon.stop();
    }

    /// The CLI compiles locally for the daemon's reason, then releases the daemon's local slot.
    #[test]
    fn test_a_cli_compiles_locally_and_releases_the_daemons_slot() {
        let fixture = one_local_slot(Eligibility::Ineligible);
        let daemon = TestDaemon::start(fixture.executor(), fixture.dir.path().join("daemon"));
        let executor = cli_executor(&fixture, &daemon.addr);
        let (dist_type, output) = fixture.compile_with(&executor);
        assert_eq!(compiled_locally(CompileLocallyReason::Staticlib), dist_type);
        assert_eq!(b"out", output.stdout.as_slice());
        // The CLI's connection stays open, so only a release frees the slot.
        let mut other = daemon.connect();
        let (compiled_tx, compiled_rx) = std::sync::mpsc::channel();
        let compiling = std::thread::spawn(move || {
            compiled_tx
                .send(send(&mut other, staticlib_request()))
                .unwrap();
            other
        });
        compiled_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("the CLI didn't release the daemon's local slot");
        drop((executor, compiling.join().unwrap()));
        daemon.stop();
    }

    /// A CLI's executor connected to a daemon that answers its setup request with `setup`, and
    /// closes the connection at any other request.
    fn cli_executor_answered_by(
        fixture: &Fixture,
        setup: ReapiExecutorSetup,
    ) -> (ReapiIpcExecutor, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = crate::net::SocketAddr::Net(listener.local_addr().unwrap());
        let daemon = std::thread::spawn(move || {
            crate::test::utils::answer_until(listener, |request| {
                matches!(
                    request,
                    Request::Reapi(ReapiIpcRequest::GetReapiExecutorSetup)
                )
                .then_some(Response::Reapi(ReapiIpcResponse::ReapiExecutorSetup(setup)))
            });
        });
        (cli_executor(fixture, &addr), daemon)
    }

    #[test]
    fn test_a_cli_compiles_locally_when_the_daemon_fails_mid_compile() {
        let fixture = Fixture::new(Eligibility::Eligible);
        let (executor, daemon) = cli_executor_answered_by(&fixture, ReapiExecutorSetup::Ready);
        assert!(fixture.runtime.block_on(executor.has_reapi_executor()));
        let (dist_type, output) = fixture.compile_with(&executor);
        assert_eq!(
            compiled_locally(CompileLocallyReason::DaemonFailed),
            dist_type
        );
        assert_eq!(b"out", output.stdout.as_slice());
        drop(executor);
        daemon.join().unwrap();
    }

    #[test]
    fn test_cli_sysroot_copies_go_under_its_dist_cache_dir() {
        let fixture = Fixture::new(Eligibility::Eligible);
        let (executor, daemon) = cli_executor_answered_by(&fixture, ReapiExecutorSetup::Ready);
        let changes = RemoteExecutor::local_compile_changes(&executor, &fixture.compilation());
        let mut expected = std::ffi::OsString::from("--sysroot=");
        expected.push(fixture.dir.path().join("cli-dist/reapi-sysroot-copies"));
        let args: Vec<_> = changes.args.into_iter().map(|arg| arg.arg).collect();
        assert_eq!(vec![expected], args);
        drop(executor);
        daemon.join().unwrap();
    }

    /// A service with a `ReapiIpcExecutor` runs remote compiles through the daemon's REAPI
    /// executor, if it has one.
    #[test_case::test_case(ReapiExecutorSetup::Ready, true ; "ready")]
    #[test_case::test_case(ReapiExecutorSetup::NoReapiExecutor, false ; "no reapi executor")]
    fn test_service_uses_the_daemons_reapi_executor(setup: ReapiExecutorSetup, uses_it: bool) {
        use crate::mock_command::MockCommandCreator;
        use crate::server::SccacheService;

        let fixture = Fixture::new(Eligibility::Eligible);
        let (executor, daemon) = cli_executor_answered_by(&fixture, setup);
        let storage = Arc::new(crate::test::mock_storage::MockStorage::new(None, false));
        let service: SccacheService<Arc<Mutex<MockCommandCreator>>> =
            SccacheService::mock_with_storage(storage, fixture.runtime.handle().clone())
                .with_reapi_ipc_executor(Arc::new(executor));
        let remote_executor = fixture.runtime.block_on(service.remote_executor()).unwrap();
        assert_eq!(uses_it, remote_executor.is_some());
        drop((remote_executor, service));
        daemon.join().unwrap();
    }

    /// With sccache-dist configured too, a CLI whose daemon can't answer compiles locally.
    #[cfg(feature = "dist-client")]
    #[test]
    fn test_service_compiles_locally_when_the_daemon_cant_answer() {
        use crate::compiler::DistIpcExecutor;
        use crate::mock_command::MockCommandCreator;
        use crate::server::SccacheService;

        let fixture = Fixture::new(Eligibility::Eligible);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = crate::net::SocketAddr::Net(listener.local_addr().unwrap());
        let daemon = std::thread::spawn(move || {
            crate::test::utils::answer_until(listener, |_| None);
        });
        let conn = Arc::new(Mutex::new(connect(&addr)));
        let storage = Arc::new(crate::test::mock_storage::MockStorage::new(None, false));
        let reapi_executor = ReapiIpcExecutor::new(conn.clone(), fixture.dir.path());
        let service: SccacheService<Arc<Mutex<MockCommandCreator>>> =
            SccacheService::mock_with_storage(storage, fixture.runtime.handle().clone())
                .with_dist_ipc_executor(Arc::new(DistIpcExecutor::new(conn)))
                .with_reapi_ipc_executor(Arc::new(reapi_executor));
        let remote_executor = fixture.runtime.block_on(service.remote_executor()).unwrap();
        assert!(remote_executor.is_none());
        daemon.join().unwrap();
    }
}
