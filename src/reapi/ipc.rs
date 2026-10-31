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
//! executor compiles it through REAPI, or tells the CLI to compile it locally.

use super::executor::{CompileLocally, ReapiExecutor};
use super::mapping::{CompileLocallyReason, ReapiCompile};
use crate::compiler::ReapiOutcome;
use crate::dist::ProcessOutput;
use serde::{Deserialize, Serialize};

/// A request for the daemon's REAPI executor.
#[derive(Debug, Serialize, Deserialize)]
pub enum ReapiIpcRequest {
    GetReapiExecutorSetup,
    CompileThroughReapi {
        /// The miss mapped for REAPI, or why it compiles locally.
        mapped: std::result::Result<Box<ReapiCompile>, CompileLocallyReason>,
        out_pretty: String,
    },
}

/// The daemon's answer to a [`ReapiIpcRequest`].
#[derive(Debug, Serialize, Deserialize)]
pub enum ReapiIpcResponse {
    ReapiExecutorSetup(ReapiExecutorSetup),
    CompileThroughReapi(CompileThroughReapiResult),
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

/// Answers `request` with the daemon's REAPI executor.
pub(crate) async fn handle_reapi_ipc_request(
    executor: Option<&ReapiExecutor>,
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
            let Some(executor) = executor else {
                return ReapiIpcResponse::CompileThroughReapi(
                    CompileThroughReapiResult::NoReapiExecutor,
                );
            };
            let mapped = mapped.map(|reapi_compile| *reapi_compile);
            let result = match executor.compile_through_reapi(mapped, &out_pretty).await {
                Ok((outcome, output)) => match output.status.code() {
                    Some(code) => CompileThroughReapiResult::FromReapi {
                        outcome,
                        output: ProcessOutput {
                            code,
                            stdout: output.stdout,
                            stderr: output.stderr,
                        },
                    },
                    // `materialize` builds the status from the action's exit code, so an output
                    // without one is a bug in sccache.
                    None => {
                        warn!("[{out_pretty}]: REAPI: the remote output has no exit code");
                        CompileThroughReapiResult::CompileLocally(
                            CompileLocallyReason::DaemonFailed,
                        )
                    }
                },
                Err(CompileLocally { reason, .. }) => {
                    CompileThroughReapiResult::CompileLocally(reason)
                }
            };
            ReapiIpcResponse::CompileThroughReapi(result)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::compiler::Compilation;
    use crate::reapi::executor::test::{Eligibility, Fixture, RLIB};

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

    #[tokio::test]
    async fn test_daemon_without_a_reapi_executor_answers_no_reapi_executor() {
        let setup = handle_reapi_ipc_request(None, ReapiIpcRequest::GetReapiExecutorSetup).await;
        assert!(
            matches!(
                setup,
                ReapiIpcResponse::ReapiExecutorSetup(ReapiExecutorSetup::NoReapiExecutor)
            ),
            "{setup:?}"
        );
        let compiled = handle_reapi_ipc_request(None, staticlib_request()).await;
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
}
