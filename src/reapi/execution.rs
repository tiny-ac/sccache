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

//! Executes actions on a REAPI server's workers.

use super::client::{AuthChannel, Client, RPC_TIMEOUT, RpcError, with_timeout};
use super::proto::build::bazel::remote::execution::v2 as remexec;
use super::proto::google::longrunning::{Operation, operation};
use super::proto::google::rpc;
use crate::errors::*;
use prost::Message;
use remexec::execution_client::ExecutionClient;
use std::time::Duration;

/// Reattaches in a row without the server reporting progress before giving up.
const MAX_REATTACHES: u32 = 3;
/// The wait before the first reattach, doubled before each next one. Short in tests, which
/// break streams on purpose.
const REATTACH_BACKOFF: Duration = if cfg!(test) {
    Duration::from_millis(1)
} else {
    Duration::from_millis(500)
};

/// Error for an Execute the server refused because its CAS is missing blobs the action
/// needs.
#[derive(Debug)]
pub(crate) struct MissingActionBlobs(String);

impl std::fmt::Display for MissingActionBlobs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the REAPI CAS is missing blobs the action needs: {}",
            self.0
        )
    }
}

impl std::error::Error for MissingActionBlobs {}

/// The Execution API of the REAPI server behind a [`Client`].
pub(crate) struct Execution {
    instance_name: String,
    execution_client: ExecutionClient<AuthChannel>,
}

impl Execution {
    pub(crate) fn new(client: &Client) -> Self {
        Execution {
            instance_name: client.instance_name().to_owned(),
            execution_client: ExecutionClient::new(client.channel()),
        }
    }

    /// Runs the action on a worker, or answers from the action cache unless
    /// `skip_cache_lookup`. Reattaches after a broken stream. Fails with
    /// [`MissingActionBlobs`] when the CAS is missing blobs the action needs. The caller
    /// bounds how long it takes.
    pub(crate) async fn execute_action(
        &self,
        action_digest: &remexec::Digest,
        skip_cache_lookup: bool,
    ) -> Result<remexec::ExecuteResponse> {
        let description = format!(
            "failed to execute action {}/{} on the REAPI server",
            action_digest.hash, action_digest.size_bytes
        );
        let mut operation_name: Option<String> = None;
        let mut reattaches_without_progress = 0;
        loop {
            let stream = match operation_name.clone() {
                Some(name) => match self.wait_execution_stream(name).await {
                    // The server lost the operation.
                    Err(e) if rpc_code(&e) == Some(tonic::Code::NotFound) => {
                        operation_name = None;
                        self.execute_stream(action_digest, skip_cache_lookup).await
                    }
                    waited => waited,
                },
                None => self.execute_stream(action_digest, skip_cache_lookup).await,
            };
            let last_error = match stream {
                Ok(mut stream) => loop {
                    match stream.message().await {
                        Ok(Some(operation)) if operation.done => {
                            return execute_response(operation.result).context(description);
                        }
                        Ok(Some(operation)) => {
                            reattaches_without_progress = 0;
                            if !operation.name.is_empty() {
                                operation_name = Some(operation.name);
                            }
                        }
                        Ok(None) => {
                            break anyhow!("the stream ended before the operation finished");
                        }
                        Err(status) => break execution_status_error(status),
                    }
                },
                Err(e) => e,
            };
            if !may_succeed_on_retry(&last_error) {
                return Err(last_error.context(description));
            }
            if reattaches_without_progress == MAX_REATTACHES {
                return Err(last_error.context(format!(
                    "{description}: no progress after {MAX_REATTACHES} reattaches"
                )));
            }
            debug!(
                "REAPI: reattaching to action {}/{} after: {last_error:#}",
                action_digest.hash, action_digest.size_bytes
            );
            tokio::time::sleep(REATTACH_BACKOFF * (1 << reattaches_without_progress)).await;
            reattaches_without_progress += 1;
        }
    }

    async fn execute_stream(
        &self,
        action_digest: &remexec::Digest,
        skip_cache_lookup: bool,
    ) -> Result<tonic::Streaming<Operation>> {
        let request = remexec::ExecuteRequest {
            instance_name: self.instance_name.clone(),
            skip_cache_lookup,
            action_digest: Some(action_digest.clone()),
            digest_function: remexec::digest_function::Value::Sha256 as i32,
            inline_stdout: true,
            inline_stderr: true,
            ..Default::default()
        };
        with_timeout("the Execute call", RPC_TIMEOUT, async {
            match self.execution_client.clone().execute(request).await {
                Ok(response) => Ok(response.into_inner()),
                Err(status) => Err(execution_status_error(status)),
            }
        })
        .await
    }

    async fn wait_execution_stream(&self, name: String) -> Result<tonic::Streaming<Operation>> {
        let request = remexec::WaitExecutionRequest { name };
        with_timeout("the WaitExecution call", RPC_TIMEOUT, async {
            match self.execution_client.clone().wait_execution(request).await {
                Ok(response) => Ok(response.into_inner()),
                Err(status) => Err(execution_status_error(status)),
            }
        })
        .await
    }
}

/// Whether reattaching or executing again may get past `e`, as after a dropped connection,
/// busy workers or a timeout.
fn may_succeed_on_retry(e: &Error) -> bool {
    if e.is::<MissingActionBlobs>() {
        return false;
    }
    match rpc_code(e) {
        Some(code) => matches!(
            code,
            tonic::Code::Unavailable
                | tonic::Code::Internal
                | tonic::Code::Unknown
                | tonic::Code::Cancelled
        ),
        None => true,
    }
}

fn rpc_code(e: &Error) -> Option<tonic::Code> {
    e.downcast_ref::<RpcError>().map(|e| e.code)
}

/// [`rpc_status_error`] for a failed Execution API call, whose details carry the
/// `google.rpc.Status`'s details.
fn execution_status_error(status: tonic::Status) -> Error {
    let details = rpc::Status::decode(status.details())
        .map(|details| details.details)
        .unwrap_or_default();
    rpc_status_error(rpc::Status {
        code: status.code() as i32,
        message: status.message().to_owned(),
        details,
    })
}

/// [`MissingActionBlobs`] for a status that reports missing blobs, and [`RpcError`]
/// otherwise.
fn rpc_status_error(status: rpc::Status) -> Error {
    if status.code == tonic::Code::FailedPrecondition as i32
        && reports_missing_blobs(&status.details)
    {
        return Error::new(MissingActionBlobs(status.message));
    }
    Error::new(RpcError {
        code: tonic::Code::from(status.code),
        message: status.message,
    })
}

/// Missing blobs come as FAILED_PRECONDITION with a MISSING PreconditionFailure.
fn reports_missing_blobs(details: &[prost_types::Any]) -> bool {
    details
        .iter()
        .filter(|detail| detail.type_url.ends_with("/google.rpc.PreconditionFailure"))
        .filter_map(|detail| rpc::PreconditionFailure::decode(detail.value.as_slice()).ok())
        .any(|failure| {
            failure
                .violations
                .iter()
                .any(|violation| violation.r#type == "MISSING")
        })
}

/// The response in a finished operation's `result`, with a result.
fn execute_response(result: Option<operation::Result>) -> Result<remexec::ExecuteResponse> {
    let response = match result {
        Some(operation::Result::Response(any)) => {
            remexec::ExecuteResponse::decode(any.value.as_slice())
                .context("failed to decode the ExecuteResponse")?
        }
        Some(operation::Result::Error(status)) => return Err(rpc_status_error(status)),
        None => bail!("the operation finished without a result"),
    };
    if let Some(status) = response.status.clone().filter(|status| status.code != 0) {
        let e = rpc_status_error(status);
        return Err(if response.message.is_empty() {
            e
        } else {
            e.context(format!("the REAPI server says: {}", response.message))
        });
    }
    if response.result.is_none() {
        bail!("the ExecuteResponse has no result");
    }
    Ok(response)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::reapi::digest;
    use crate::reapi::fake::{FakeServer, StreamBreak};
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};
    use test_case::test_case;

    /// A server whose workers write `out`.
    fn writing_server() -> FakeServer {
        FakeServer {
            worker_outputs: Mutex::new([("out".to_owned(), b"x".to_vec())].into()),
            ..Default::default()
        }
    }

    /// Puts an action that declares `out` in the server's CAS, and returns its digest.
    fn insert_action(server: &FakeServer) -> remexec::Digest {
        let command = remexec::Command {
            arguments: vec!["true".to_owned()],
            output_paths: vec!["out".to_owned()],
            ..Default::default()
        };
        let action = remexec::Action {
            command_digest: Some(server.insert_blob(&command.encode_to_vec())),
            ..Default::default()
        };
        server.insert_blob(&action.encode_to_vec())
    }

    async fn execute(server: FakeServer) -> Result<remexec::ExecuteResponse> {
        let server = Arc::new(server);
        let execution = Execution::new(&server.serve_and_connect().await);
        execution
            .execute_action(&insert_action(&server), false)
            .await
    }

    #[tokio::test]
    async fn test_execute_answers_from_the_action_cache_unless_skip_cache_lookup() {
        let server = Arc::new(FakeServer {
            instance_name: "main".to_owned(),
            ..writing_server()
        });
        let execution = Execution::new(&server.serve_and_connect().await);
        let action_digest = insert_action(&server);
        let response = execution
            .execute_action(&action_digest, false)
            .await
            .unwrap();
        assert!(!response.cached_result);
        assert_eq!("out", response.result.unwrap().output_files[0].path);
        let response = execution
            .execute_action(&action_digest, false)
            .await
            .unwrap();
        assert!(response.cached_result);
        execution
            .execute_action(&action_digest, true)
            .await
            .unwrap();
        assert_eq!(2, server.actions_run.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_an_action_the_cas_is_missing_is_missing_action_blobs() {
        let server = Arc::new(FakeServer::default());
        let execution = Execution::new(&server.serve_and_connect().await);
        let e = execution
            .execute_action(&digest::of_bytes(b"no such action"), false)
            .await
            .unwrap_err();
        assert!(e.is::<MissingActionBlobs>(), "{e:#}");
    }

    #[tokio::test]
    async fn test_missing_blobs_in_the_response_are_missing_action_blobs() {
        let e = execute(FakeServer {
            missing_blobs_in_response: true,
            ..writing_server()
        })
        .await
        .unwrap_err();
        assert!(e.is::<MissingActionBlobs>(), "{e:#}");
    }

    #[tokio::test]
    async fn test_a_failed_precondition_without_missing_blobs_keeps_its_code() {
        let e = execute(FakeServer {
            without_matching_worker: true,
            ..writing_server()
        })
        .await
        .unwrap_err();
        assert!(!e.is::<MissingActionBlobs>(), "{e:#}");
        assert_eq!(Some(tonic::Code::FailedPrecondition), rpc_code(&e));
    }

    #[tokio::test]
    async fn test_a_failed_operation_keeps_its_code() {
        let e = execute(FakeServer {
            fail_operations: true,
            ..writing_server()
        })
        .await
        .unwrap_err();
        assert_eq!(Some(tonic::Code::Internal), rpc_code(&e));
    }

    #[tokio::test]
    async fn test_a_broken_stream_reattaches() {
        let server = Arc::new(FakeServer {
            streams_to_break: 1.into(),
            keep_operations: true,
            ..writing_server()
        });
        let execution = Execution::new(&server.serve_and_connect().await);
        let response = execution
            .execute_action(&insert_action(&server), false)
            .await
            .unwrap();
        assert_eq!("out", response.result.unwrap().output_files[0].path);
        assert_eq!(1, server.execute_calls.load(Ordering::SeqCst));
        assert_eq!(1, server.wait_execution_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_an_operation_the_server_lost_executes_again() {
        let server = Arc::new(FakeServer {
            streams_to_break: 1.into(),
            ..writing_server()
        });
        let execution = Execution::new(&server.serve_and_connect().await);
        execution
            .execute_action(&insert_action(&server), false)
            .await
            .unwrap();
        assert_eq!(1, server.wait_execution_calls.load(Ordering::SeqCst));
        assert_eq!(2, server.execute_calls.load(Ordering::SeqCst));
    }

    #[test_case(tonic::Code::Unavailable ; "unavailable")]
    #[test_case(tonic::Code::Internal ; "internal")]
    #[test_case(tonic::Code::Unknown ; "unknown")]
    #[test_case(tonic::Code::Cancelled ; "cancelled")]
    #[tokio::test]
    async fn test_a_stream_broken_with_a_transient_code_reattaches(code: tonic::Code) {
        execute(FakeServer {
            streams_to_break: 1.into(),
            stream_break: StreamBreak::Status(code),
            ..writing_server()
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_a_stream_broken_with_another_code_fails() {
        let server = Arc::new(FakeServer {
            streams_to_break: 1.into(),
            stream_break: StreamBreak::Status(tonic::Code::PermissionDenied),
            ..writing_server()
        });
        let execution = Execution::new(&server.serve_and_connect().await);
        let e = execution
            .execute_action(&insert_action(&server), false)
            .await
            .unwrap_err();
        assert_eq!(Some(tonic::Code::PermissionDenied), rpc_code(&e));
        assert_eq!(1, server.execute_calls.load(Ordering::SeqCst));
    }

    #[test_case(MAX_REATTACHES as usize, true ; "as many as it reattaches")]
    #[test_case(MAX_REATTACHES as usize + 1, false ; "one more")]
    #[tokio::test]
    async fn test_streams_without_progress_are_reattached_up_to_max_reattaches(
        broken_streams: usize,
        finishes: bool,
    ) {
        let executed = execute(FakeServer {
            streams_to_break: broken_streams.into(),
            break_before_operation: true,
            ..writing_server()
        })
        .await;
        assert_eq!(finishes, executed.is_ok(), "{executed:?}");
        if let Err(e) = executed {
            assert_eq!(Some(tonic::Code::Unavailable), rpc_code(&e));
        }
    }

    #[tokio::test]
    async fn test_streams_that_make_progress_reattach_past_max_reattaches() {
        execute(FakeServer {
            streams_to_break: (MAX_REATTACHES as usize + 2).into(),
            keep_operations: true,
            ..writing_server()
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_streams_that_end_unfinished_say_so() {
        let e = execute(FakeServer {
            streams_to_break: (MAX_REATTACHES as usize + 1).into(),
            stream_break: StreamBreak::End,
            break_before_operation: true,
            ..writing_server()
        })
        .await
        .unwrap_err();
        assert!(
            format!("{e:#}").contains("the stream ended before the operation finished"),
            "{e:#}"
        );
    }

    #[tokio::test]
    async fn test_refused_calls_are_retried() {
        let server = Arc::new(FakeServer {
            executes_to_refuse: 1.into(),
            streams_to_break: 1.into(),
            keep_operations: true,
            wait_executions_to_refuse: 1.into(),
            ..writing_server()
        });
        let execution = Execution::new(&server.serve_and_connect().await);
        execution
            .execute_action(&insert_action(&server), false)
            .await
            .unwrap();
        assert_eq!(2, server.execute_calls.load(Ordering::SeqCst));
        assert_eq!(2, server.wait_execution_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_blobs_lost_with_the_operation_are_missing_action_blobs() {
        let server = Arc::new(FakeServer {
            streams_to_break: 1.into(),
            forget_blobs_on_break: true,
            ..writing_server()
        });
        let execution = Execution::new(&server.serve_and_connect().await);
        let e = execution
            .execute_action(&insert_action(&server), false)
            .await
            .unwrap_err();
        assert!(e.is::<MissingActionBlobs>(), "{e:#}");
        // The first Execute and one more, with no reattaches after it.
        assert_eq!(2, server.execute_calls.load(Ordering::SeqCst));
    }
}
