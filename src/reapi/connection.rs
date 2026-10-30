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

//! The connection to a REAPI server. The first compile that needs the server starts
//! connecting in a separate task, and no compile waits for that task past its deadline.
//! After a failed attempt, the first compile after a backoff connects again.

use super::action_cache::ActionCache;
use super::cas::Cas;
use super::client::{Client, ConnectOptions, CredentialsRefused, IncompatibleServer};
use super::execution::Execution;
use super::mapping::CompileLocallyReason;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::watch;

const BACKOFF_MIN: Duration = Duration::from_secs(5);
const BACKOFF_MAX: Duration = Duration::from_secs(60);

pub(super) struct ServerApis {
    pub(super) cas: Cas,
    pub(super) action_cache: ActionCache,
    /// `None` for a server that doesn't execute actions.
    #[expect(dead_code, reason = "compiles don't execute yet")]
    pub(super) execution: Option<Execution>,
}

enum State {
    NotConnected,
    Connecting,
    Connected(Arc<ServerApis>),
    ConnectFailed {
        retry_at: Instant,
    },
    /// Compiles run locally until the sccache server restarts, since retrying with the same
    /// server and the same token can't succeed.
    Refused(CompileLocallyReason),
}

pub(super) struct Connection {
    options: ConnectOptions,
    state: tokio::sync::Mutex<State>,
    failures_in_a_row: AtomicU32,
    attempts_ended: watch::Sender<u64>,
}

impl Connection {
    pub(super) fn new(options: ConnectOptions) -> Arc<Self> {
        Arc::new(Connection {
            options,
            state: tokio::sync::Mutex::new(State::NotConnected),
            failures_in_a_row: AtomicU32::new(0),
            attempts_ended: watch::Sender::new(0),
        })
    }

    /// The server's APIs, or why a compile runs locally instead. Waits for a connection
    /// up to `deadline`.
    pub(super) async fn server_apis(
        self: &Arc<Self>,
        deadline: Instant,
    ) -> Result<Arc<ServerApis>, CompileLocallyReason> {
        let mut attempts_ended = self.attempts_ended.subscribe();
        loop {
            {
                let mut state = self.state.lock().await;
                match &*state {
                    State::Connected(apis) => return Ok(apis.clone()),
                    State::Refused(reason) => return Err(*reason),
                    State::ConnectFailed { retry_at } if Instant::now() < *retry_at => {
                        return Err(CompileLocallyReason::ConnectFailed);
                    }
                    State::Connecting => {}
                    State::NotConnected | State::ConnectFailed { .. } => {
                        *state = State::Connecting;
                        self.spawn_connect();
                    }
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero()
                || tokio::time::timeout(remaining, attempts_ended.changed())
                    .await
                    .is_err()
            {
                return Err(CompileLocallyReason::Connecting);
            }
        }
    }

    fn spawn_connect(self: &Arc<Self>) {
        let this = self.clone();
        tokio::spawn(async move {
            let state = tokio::spawn(this.clone().connect())
                .await
                .unwrap_or_else(|e| {
                    if e.is_panic() {
                        warn!("REAPI: connecting panicked: {e}");
                    }
                    this.connect_failed()
                });
            *this.state.lock().await = state;
            this.attempts_ended.send_modify(|count| *count += 1);
        });
    }

    async fn connect(self: Arc<Self>) -> State {
        match Client::connect(&self.options).await {
            Ok(client) => {
                self.failures_in_a_row.store(0, Ordering::Relaxed);
                State::Connected(Arc::new(ServerApis {
                    cas: Cas::new(&client),
                    action_cache: ActionCache::new(&client),
                    execution: client.executes_actions().then(|| Execution::new(&client)),
                }))
            }
            Err(e) if e.is::<IncompatibleServer>() || e.is::<CredentialsRefused>() => {
                warn!("REAPI: {e:#}; compiling locally until the sccache server restarts");
                State::Refused(if e.is::<IncompatibleServer>() {
                    CompileLocallyReason::IncompatibleServer
                } else {
                    CompileLocallyReason::CredentialsRefused
                })
            }
            Err(e) => {
                let state = self.connect_failed();
                warn!("REAPI: {e:#}; compiling locally for {:?}", self.backoff());
                state
            }
        }
    }

    fn connect_failed(&self) -> State {
        self.failures_in_a_row.fetch_add(1, Ordering::Relaxed);
        State::ConnectFailed {
            retry_at: Instant::now() + self.backoff(),
        }
    }

    /// `BACKOFF_MIN`, doubled for each failure in a row after the first, up to `BACKOFF_MAX`.
    fn backoff(&self) -> Duration {
        let failures = self.failures_in_a_row.load(Ordering::Relaxed);
        BACKOFF_MIN
            .saturating_mul(2u32.saturating_pow(failures.saturating_sub(1)))
            .min(BACKOFF_MAX)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::reapi::fake::FakeServer;

    async fn serve(server: FakeServer) -> (Arc<FakeServer>, Arc<Connection>) {
        let server = Arc::new(server);
        let connection = Connection::new(ConnectOptions {
            endpoint: server.serve().await,
            ..Default::default()
        });
        (server, connection)
    }

    fn failing_with(code: tonic::Code) -> FakeServer {
        FakeServer {
            capabilities_error: Some(code).into(),
            ..Default::default()
        }
    }

    fn in_secs(secs: u64) -> Instant {
        Instant::now() + Duration::from_secs(secs)
    }

    fn capabilities_calls(server: &FakeServer) -> usize {
        server.capabilities_calls.load(Ordering::SeqCst)
    }

    #[tokio::test]
    async fn test_a_compile_that_doesnt_wait_finds_the_server_connecting() {
        let (_, connection) = serve(FakeServer::default()).await;
        assert_eq!(
            Some(CompileLocallyReason::Connecting),
            connection.server_apis(Instant::now()).await.err()
        );
        assert!(connection.server_apis(in_secs(10)).await.is_ok());
        assert!(connection.server_apis(Instant::now()).await.is_ok());
    }

    #[tokio::test]
    async fn test_compiles_at_once_share_one_connect() {
        let (server, connection) = serve(FakeServer::default()).await;
        let compiles: Vec<_> = (0..4)
            .map(|_| {
                let connection = connection.clone();
                tokio::spawn(async move { connection.server_apis(in_secs(10)).await.is_ok() })
            })
            .collect();
        for compile in compiles {
            assert!(compile.await.unwrap());
        }
        assert_eq!(1, capabilities_calls(&server));
    }

    #[tokio::test]
    async fn test_a_failed_connect_is_retried_after_the_backoff() {
        let (server, connection) = serve(failing_with(tonic::Code::Unavailable)).await;
        assert_eq!(
            Some(CompileLocallyReason::ConnectFailed),
            connection.server_apis(in_secs(10)).await.err()
        );
        assert_eq!(
            Some(CompileLocallyReason::ConnectFailed),
            connection.server_apis(in_secs(10)).await.err()
        );
        assert_eq!(1, capabilities_calls(&server), "retried during the backoff");

        *server.capabilities_error.lock().unwrap() = None;
        *connection.state.lock().await = State::ConnectFailed {
            retry_at: Instant::now(),
        };
        assert!(connection.server_apis(in_secs(10)).await.is_ok());
        assert_eq!(2, capabilities_calls(&server));
        assert_eq!(0, connection.failures_in_a_row.load(Ordering::Relaxed));
    }

    #[test_case::test_case(
        failing_with(tonic::Code::Unauthenticated),
        CompileLocallyReason::CredentialsRefused ;
        "credentials refused"
    )]
    #[test_case::test_case(
        FakeServer { without_caching: true, ..Default::default() },
        CompileLocallyReason::IncompatibleServer ;
        "incompatible server"
    )]
    #[tokio::test]
    async fn test_a_refusing_server_is_never_retried(
        server: FakeServer,
        reason: CompileLocallyReason,
    ) {
        let (server, connection) = serve(server).await;
        for _ in 0..2 {
            assert_eq!(
                Some(reason),
                connection.server_apis(in_secs(10)).await.err()
            );
        }
        assert_eq!(1, capabilities_calls(&server));
    }

    #[test]
    fn test_backoffs_double_up_to_the_max() {
        let connection = Connection::new(Default::default());
        let backoffs: Vec<_> = (0..6)
            .map(|_| {
                connection.connect_failed();
                connection.backoff().as_secs()
            })
            .collect();
        assert_eq!(vec![5, 10, 20, 40, 60, 60], backoffs);
    }
}
