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

//! Looks actions up in a REAPI server's action cache.

use super::client::{AuthChannel, Client, RPC_TIMEOUT, status_error, with_retries, with_timeout};
use super::proto::build::bazel::remote::execution::v2 as remexec;
use crate::errors::*;
use remexec::action_cache_client::ActionCacheClient;

/// The action cache of the REAPI server behind a [`Client`].
pub(crate) struct ActionCache {
    instance_name: String,
    action_cache_client: ActionCacheClient<AuthChannel>,
}

impl ActionCache {
    pub(crate) fn new(client: &Client) -> Self {
        ActionCache {
            instance_name: client.instance_name().to_owned(),
            action_cache_client: ActionCacheClient::new(client.channel()),
        }
    }

    /// The action cache's result for `action_digest`, or `None` on a miss.
    pub(crate) async fn get_action_result(
        &self,
        action_digest: &remexec::Digest,
    ) -> Result<Option<remexec::ActionResult>> {
        let request = remexec::GetActionResultRequest {
            instance_name: self.instance_name.clone(),
            action_digest: Some(action_digest.clone()),
            inline_stdout: true,
            inline_stderr: true,
            inline_output_files: vec![],
            digest_function: remexec::digest_function::Value::Sha256 as i32,
        };
        with_timeout(
            &format!(
                "failed to look up action {}/{} in the REAPI action cache",
                action_digest.hash, action_digest.size_bytes
            ),
            RPC_TIMEOUT,
            with_retries(|| async {
                match self
                    .action_cache_client
                    .clone()
                    .get_action_result(request.clone())
                    .await
                {
                    Ok(response) => Ok(Some(response.into_inner())),
                    Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
                    Err(status) => Err(status_error(status)),
                }
            }),
        )
        .await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::reapi::client::{RETRIES, rpc_code};
    use crate::reapi::digest;
    use crate::reapi::fake::FakeServer;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_a_miss_is_none() {
        let server = Arc::new(FakeServer::default());
        let action_cache = ActionCache::new(&server.serve_and_connect().await);
        assert!(
            action_cache
                .get_action_result(&digest::of_bytes(b"action"))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_a_cached_result_is_found() {
        let server = Arc::new(FakeServer::default());
        let action_cache = ActionCache::new(&server.serve_and_connect().await);
        let action_digest = digest::of_bytes(b"action");
        let result = remexec::ActionResult {
            exit_code: 1,
            ..Default::default()
        };
        server.insert_action_result(&action_digest, result.clone());
        assert_eq!(
            Some(result),
            action_cache
                .get_action_result(&action_digest)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_lookups_are_tried_again_while_the_server_is_busy() {
        let server = Arc::new(FakeServer::default());
        let action_cache = ActionCache::new(&server.serve_and_connect().await);
        let action_digest = digest::of_bytes(b"action");
        server.insert_action_result(&action_digest, remexec::ActionResult::default());
        server.answer_busy("GetActionResult", RETRIES as usize);
        assert!(
            action_cache
                .get_action_result(&action_digest)
                .await
                .unwrap()
                .is_some()
        );
        assert!(server.gave_every_busy_answer());
    }

    #[tokio::test]
    async fn test_lookups_use_the_instance_name() {
        let server = Arc::new(FakeServer {
            instance_name: "main".to_owned(),
            ..Default::default()
        });
        let action_cache = ActionCache::new(&server.serve_and_connect().await);
        let action_digest = digest::of_bytes(b"action");
        let result = remexec::ActionResult::default();
        server.insert_action_result(&action_digest, result.clone());
        assert_eq!(
            Some(result),
            action_cache
                .get_action_result(&action_digest)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_a_failed_lookup_is_an_error() {
        let server = Arc::new(FakeServer::default());
        let action_cache = ActionCache::new(&server.serve_and_connect().await);
        *server.lookup_error.lock().unwrap() = Some(tonic::Code::Internal);
        let e = action_cache
            .get_action_result(&digest::of_bytes(b"action"))
            .await
            .unwrap_err();
        assert_eq!(Some(tonic::Code::Internal), rpc_code(&e), "{e:#}");
    }
}
