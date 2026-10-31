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

use super::executor::{CompileLocally, ReapiExecutor};
use super::mapping::{CompileLocallyReason, ReapiCompile};
use crate::compiler::ReapiOutcome;
use crate::dist::ProcessOutput;
use serde::{Deserialize, Serialize};
use std::process;
use tokio::sync::OwnedSemaphorePermit;

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
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::client::ServerConnection;
    use crate::compiler::Compilation;
    use crate::protocol::{Request, Response};
    use crate::reapi::executor::test::{Eligibility, Fixture, RLIB};
    use std::num::NonZeroUsize;
    use std::path::PathBuf;
    use std::sync::Arc;
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
    fn one_local_slot() -> Fixture {
        let mut fixture = Fixture::new(Eligibility::Eligible);
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
        let fixture = one_local_slot();
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
        let fixture = one_local_slot();
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

        let fixture = one_local_slot();
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
        let fixture = one_local_slot();
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
}
