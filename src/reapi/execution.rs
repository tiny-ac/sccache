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
    /// `skip_cache_lookup`. Fails with [`MissingActionBlobs`] when the CAS is missing blobs
    /// the action needs. The caller bounds how long it takes.
    pub(crate) async fn execute_action(
        &self,
        action_digest: &remexec::Digest,
        skip_cache_lookup: bool,
    ) -> Result<remexec::ExecuteResponse> {
        let description = format!(
            "failed to execute action {}/{} on the REAPI server",
            action_digest.hash, action_digest.size_bytes
        );
        let mut stream = self
            .execute_stream(action_digest, skip_cache_lookup)
            .await
            .with_context(|| description.clone())?;
        while let Some(operation) = stream
            .message()
            .await
            .map_err(execution_status_error)
            .with_context(|| description.clone())?
        {
            if operation.done {
                return execute_response(operation.result).with_context(|| description.clone());
            }
        }
        bail!("{description}: the Execute stream ended before the operation finished")
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
    use crate::reapi::fake::FakeServer;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    /// A server whose workers write `out`.
    fn writing_server() -> FakeServer {
        FakeServer {
            worker_outputs: [("out".to_owned(), b"x".to_vec())].into(),
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
}
