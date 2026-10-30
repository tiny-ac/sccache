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
    pub(super) execution: Option<Execution>,
    /// The version of the credentials when sccache connected.
    credentials_version: u64,
}

enum State {
    NotConnected,
    Connecting,
    Connected(Arc<ServerApis>),
    ConnectFailed {
        retry_at: Instant,
        reason: CompileLocallyReason,
    },
    /// Compiles run locally until the sccache server restarts, or, for refused credentials,
    /// until the credentials change, since retrying with the same server and the same
    /// credentials can't succeed.
    Refused(CompileLocallyReason),
}

pub(super) struct Connection {
    options: ConnectOptions,
    state: tokio::sync::Mutex<State>,
    failures_in_a_row: AtomicU32,
    attempts_ended: watch::Sender<u64>,
    /// The version of the credentials the server last refused, on a call or when connecting.
    refused_credentials_version: std::sync::Mutex<Option<u64>>,
}

impl Connection {
    pub(super) fn new(options: ConnectOptions) -> Arc<Self> {
        Arc::new(Connection {
            options,
            state: tokio::sync::Mutex::new(State::NotConnected),
            failures_in_a_row: AtomicU32::new(0),
            attempts_ended: watch::Sender::new(0),
            refused_credentials_version: Default::default(),
        })
    }

    /// The server's APIs, or why a compile runs locally instead. Waits for a connection
    /// up to `deadline`.
    pub(super) async fn server_apis(
        self: &Arc<Self>,
        deadline: Instant,
    ) -> Result<Arc<ServerApis>, CompileLocallyReason> {
        let mut attempts_ended = self.attempts_ended.subscribe();
        if let Some(auth_token) = &self.options.auth_token {
            // Read in the background; calls send the new token once the read ends.
            drop(auth_token.read_again());
        }
        loop {
            {
                let credentials_version = self.credentials_version();
                let mut state = self.state.lock().await;
                match &*state {
                    State::Connected(apis) => {
                        self.renew_helper_headers_in_background();
                        return Ok(apis.clone());
                    }
                    State::Refused(CompileLocallyReason::CredentialsRefused)
                        if *self.refused_credentials_version.lock().unwrap()
                            != Some(credentials_version) =>
                    {
                        info!("REAPI: the credentials changed; connecting again");
                        *state = State::Connecting;
                        self.spawn_connect();
                    }
                    State::Refused(reason) => {
                        if *reason == CompileLocallyReason::CredentialsRefused {
                            self.renew_helper_headers_in_background();
                        }
                        return Err(*reason);
                    }
                    State::ConnectFailed { retry_at, reason } if Instant::now() < *retry_at => {
                        return Err(*reason);
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

    /// For a server that refused the credentials on a call through `apis`. The first time for
    /// some credentials, the next compile connects again, and GetCapabilities decides, as at
    /// the first connect, since a proxy may refuse a call once. The second time, every compile
    /// runs locally until the sccache server restarts or the credentials change.
    pub(super) async fn credentials_refused(&self, apis: &Arc<ServerApis>) {
        let mut state = self.state.lock().await;
        // A call through an earlier connection says nothing about the current one.
        if !matches!(&*state, State::Connected(current) if Arc::ptr_eq(current, apis)) {
            return;
        }
        let credentials_version = self.credentials_version();
        if apis.credentials_version != credentials_version {
            // The call may have carried the earlier credentials; the new ones get their own
            // check.
            info!("REAPI: the server refused the credentials; connecting again with the new ones");
            *state = State::NotConnected;
            return;
        }
        let mut refused_credentials_version = self.refused_credentials_version.lock().unwrap();
        if *refused_credentials_version == Some(credentials_version) {
            warn!(
                "REAPI: the server refused the credentials again; compiling locally until the \
                 sccache server restarts or the credentials change"
            );
            *state = State::Refused(CompileLocallyReason::CredentialsRefused);
        } else {
            warn!("REAPI: the server refused the credentials; connecting again to check them");
            *refused_credentials_version = Some(credentials_version);
            *state = State::NotConnected;
        }
        self.expire_helper_headers();
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
                    this.connect_failed(CompileLocallyReason::ConnectFailed)
                });
            *this.state.lock().await = state;
            this.attempts_ended.send_modify(|count| *count += 1);
        });
    }

    /// How many times the auth token or the credential helper's headers changed; 0 without
    /// either. Only one of them can be configured, so the sum counts the changes of that one.
    fn credentials_version(&self) -> u64 {
        let auth_token = self.options.auth_token.as_ref();
        let credential_helper = self.options.credential_helper.as_ref();
        auth_token.map_or(0, |auth_token| auth_token.version())
            + credential_helper.map_or(0, |credential_helper| credential_helper.version())
    }

    /// Runs the credential helper in the background if its headers are due. While sccache
    /// isn't connected, `connect` runs it instead.
    fn renew_helper_headers_in_background(&self) {
        if let Some(credential_helper) = &self.options.credential_helper {
            drop(credential_helper.renew_in_background());
        }
    }

    /// Makes the credential helper run again before the next connect, since the server
    /// refused the headers it printed. A connect may have used headers it didn't renew.
    fn expire_helper_headers(&self) {
        if let Some(credential_helper) = &self.options.credential_helper {
            credential_helper.expire();
        }
    }

    async fn connect(self: Arc<Self>) -> State {
        if let Some(credential_helper) = &self.options.credential_helper
            && let Err(e) = credential_helper.renew().await
        {
            let state = self.connect_failed(CompileLocallyReason::CredentialHelperFailed);
            warn!("REAPI: {e:#}; compiling locally for {:?}", self.backoff());
            return state;
        }
        let credentials_version = self.credentials_version();
        match Client::connect(&self.options).await {
            Ok(client) => {
                self.failures_in_a_row.store(0, Ordering::Relaxed);
                State::Connected(Arc::new(ServerApis {
                    cas: Cas::new(&client),
                    action_cache: ActionCache::new(&client),
                    execution: client.executes_actions().then(|| Execution::new(&client)),
                    credentials_version,
                }))
            }
            Err(e) if e.is::<IncompatibleServer>() => {
                warn!("REAPI: {e:#}; compiling locally until the sccache server restarts");
                State::Refused(CompileLocallyReason::IncompatibleServer)
            }
            Err(e) if e.is::<CredentialsRefused>() => {
                warn!(
                    "REAPI: {e:#}; compiling locally until the sccache server restarts or the \
                     credentials change"
                );
                *self.refused_credentials_version.lock().unwrap() = Some(credentials_version);
                self.expire_helper_headers();
                State::Refused(CompileLocallyReason::CredentialsRefused)
            }
            Err(e) => {
                let state = self.connect_failed(CompileLocallyReason::ConnectFailed);
                warn!("REAPI: {e:#}; compiling locally for {:?}", self.backoff());
                state
            }
        }
    }

    fn connect_failed(&self, reason: CompileLocallyReason) -> State {
        self.failures_in_a_row.fetch_add(1, Ordering::Relaxed);
        State::ConnectFailed {
            retry_at: Instant::now() + self.backoff(),
            reason,
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
    use crate::reapi::auth_token::AuthToken;
    use crate::reapi::auth_token::test::{read_again_now, token_file};
    #[cfg(unix)]
    use crate::reapi::credential_helper::test::{runs, script_helper};
    use crate::reapi::fake::FakeServer;

    async fn serve(server: FakeServer) -> (Arc<FakeServer>, Arc<Connection>) {
        let server = Arc::new(server);
        let connection = Connection::new(ConnectOptions {
            endpoint: server.serve().await,
            ..Default::default()
        });
        (server, connection)
    }

    async fn serve_with_token(
        server: FakeServer,
        auth_token: &Arc<AuthToken>,
    ) -> (Arc<FakeServer>, Arc<Connection>) {
        let server = Arc::new(server);
        let connection = Connection::new(ConnectOptions {
            endpoint: server.serve().await,
            auth_token: Some(auth_token.clone()),
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
            reason: CompileLocallyReason::ConnectFailed,
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

    #[tokio::test]
    async fn test_refused_credentials_are_checked_again_once() {
        let (server, connection) = serve(FakeServer::default()).await;
        let apis = connection.server_apis(in_secs(10)).await.unwrap();
        connection.credentials_refused(&apis).await;
        let apis = connection.server_apis(in_secs(10)).await.unwrap();
        assert_eq!(2, capabilities_calls(&server));
        connection.credentials_refused(&apis).await;
        assert_eq!(
            Some(CompileLocallyReason::CredentialsRefused),
            connection.server_apis(in_secs(10)).await.err()
        );
        assert_eq!(2, capabilities_calls(&server));
    }

    #[tokio::test]
    async fn test_a_server_that_refused_a_token_at_connect_is_tried_with_the_next() {
        let dir = tempfile::tempdir().unwrap();
        let (auth_token, write_token) = token_file(dir.path(), Duration::ZERO);
        let (server, connection) =
            serve_with_token(failing_with(tonic::Code::Unauthenticated), &auth_token).await;
        for _ in 0..2 {
            assert_eq!(
                Some(CompileLocallyReason::CredentialsRefused),
                connection.server_apis(in_secs(10)).await.err()
            );
        }
        assert_eq!(1, capabilities_calls(&server));
        write_token("second");
        read_again_now(&auth_token).await;
        *server.capabilities_error.lock().unwrap() = None;
        assert!(connection.server_apis(in_secs(10)).await.is_ok());
        assert_eq!(2, capabilities_calls(&server));
    }

    #[tokio::test]
    async fn test_credentials_refused_twice_are_checked_again_with_the_next_token() {
        let dir = tempfile::tempdir().unwrap();
        let (auth_token, write_token) = token_file(dir.path(), Duration::ZERO);
        let (server, connection) = serve_with_token(FakeServer::default(), &auth_token).await;
        let apis = connection.server_apis(in_secs(10)).await.unwrap();
        connection.credentials_refused(&apis).await;
        let apis = connection.server_apis(in_secs(10)).await.unwrap();
        connection.credentials_refused(&apis).await;
        assert!(connection.server_apis(in_secs(10)).await.is_err());
        write_token("second");
        read_again_now(&auth_token).await;
        let apis = connection.server_apis(in_secs(10)).await.unwrap();
        assert_eq!(3, capabilities_calls(&server));
        // The new token gets its own check.
        connection.credentials_refused(&apis).await;
        assert!(connection.server_apis(in_secs(10)).await.is_ok());
        assert_eq!(4, capabilities_calls(&server));
    }

    #[tokio::test]
    async fn test_a_refusal_through_a_connection_with_an_earlier_token_doesnt_count() {
        let dir = tempfile::tempdir().unwrap();
        let (auth_token, write_token) = token_file(dir.path(), Duration::ZERO);
        let (server, connection) = serve_with_token(FakeServer::default(), &auth_token).await;
        let apis = connection.server_apis(in_secs(10)).await.unwrap();
        write_token("second");
        read_again_now(&auth_token).await;
        connection.credentials_refused(&apis).await;
        let apis = connection.server_apis(in_secs(10)).await.unwrap();
        // The first refusal of the new token.
        connection.credentials_refused(&apis).await;
        let apis = connection.server_apis(in_secs(10)).await.unwrap();
        assert_eq!(3, capabilities_calls(&server));
        connection.credentials_refused(&apis).await;
        assert_eq!(
            Some(CompileLocallyReason::CredentialsRefused),
            connection.server_apis(in_secs(10)).await.err()
        );
    }

    #[tokio::test]
    async fn test_a_compile_reads_a_changed_token_file() {
        let dir = tempfile::tempdir().unwrap();
        let (auth_token, write_token) = token_file(dir.path(), Duration::ZERO);
        let (_, connection) = serve_with_token(FakeServer::default(), &auth_token).await;
        write_token("second");
        connection.server_apis(in_secs(10)).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while auth_token.version() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the token file wasn't read again");
    }

    #[cfg(unix)]
    async fn serve_with_helper(
        server: FakeServer,
        dir: &std::path::Path,
        output: &str,
    ) -> (Arc<FakeServer>, Arc<Connection>) {
        let server = Arc::new(server);
        let connection = Connection::new(ConnectOptions {
            endpoint: server.serve().await,
            credential_helper: Some(Arc::new(script_helper(dir, output))),
            ..Default::default()
        });
        (server, connection)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_a_failing_credential_helper_keeps_compiles_local_and_waits_for_the_backoff() {
        let dir = tempfile::tempdir().unwrap();
        let (server, connection) =
            serve_with_helper(FakeServer::default(), dir.path(), "not json").await;
        for _ in 0..2 {
            assert_eq!(
                Some(CompileLocallyReason::CredentialHelperFailed),
                connection.server_apis(in_secs(10)).await.err()
            );
        }
        assert_eq!(1, runs(dir.path()));
        assert_eq!(0, capabilities_calls(&server));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_refused_helper_headers_are_fetched_again() {
        let dir = tempfile::tempdir().unwrap();
        let (server, connection) =
            serve_with_helper(FakeServer::default(), dir.path(), r#"{"headers": {}}"#).await;
        let apis = connection.server_apis(in_secs(10)).await.unwrap();
        assert_eq!(1, runs(dir.path()));
        connection.credentials_refused(&apis).await;
        connection.server_apis(in_secs(10)).await.unwrap();
        assert_eq!(2, runs(dir.path()));
        assert_eq!(2, capabilities_calls(&server));
    }

    #[tokio::test]
    async fn test_a_refusal_through_an_earlier_connection_is_ignored() {
        let (server, connection) = serve(FakeServer::default()).await;
        let earlier = connection.server_apis(in_secs(10)).await.unwrap();
        connection.credentials_refused(&earlier).await;
        connection.server_apis(in_secs(10)).await.unwrap();
        connection.credentials_refused(&earlier).await;
        assert!(connection.server_apis(in_secs(10)).await.is_ok());
        assert_eq!(2, capabilities_calls(&server));
    }

    #[test]
    fn test_backoffs_double_up_to_the_max() {
        let connection = Connection::new(Default::default());
        let backoffs: Vec<_> = (0..6)
            .map(|_| {
                connection.connect_failed(CompileLocallyReason::ConnectFailed);
                connection.backoff().as_secs()
            })
            .collect();
        assert_eq!(vec![5, 10, 20, 40, 60, 60], backoffs);
    }
}
