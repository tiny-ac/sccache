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

//! Headers from a Bazel credential helper
//! (<https://github.com/EngFlow/credential-helper-spec>): for each gRPC service, sccache runs
//! `<helper> get`, writes `{"uri": "https://<host>[:<port>]/<service>"}` to its stdin, and sends
//! the headers it prints with every call to that service, running it again before they expire.
//! Bazel sends the same URIs, and some helpers need the service to choose the credentials.

use super::client::{header_key, sensitive_ascii_value};
use crate::errors::*;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};

pub(super) const HELPER_TIMEOUT: Duration = Duration::from_secs(10);
/// How long headers without `expires` are used, as Bazel does.
const LIFETIME_WITHOUT_EXPIRY: Duration = Duration::from_secs(30 * 60);
/// How long before `expires` the helper runs again, so that a call doesn't carry headers that
/// expire on the way.
const RENEW_BEFORE_EXPIRY: Duration = Duration::from_secs(60);
/// The shortest time headers are used, so that a helper that prints a past `expires` doesn't
/// run for every compile.
const MIN_LIFETIME: Duration = Duration::from_secs(10);
/// How long after a failed run the helper isn't run again in the background, so that a failing
/// helper doesn't run for every compile. Compiles don't wait for the helper meanwhile.
const RETRY_AFTER_FAILURE: Duration = Duration::from_secs(10);
/// More than any helper prints, so that a broken one can't fill the memory.
const MAX_OUTPUT_BYTES: u64 = 1 << 20;

/// The gRPC services sccache calls.
pub(super) const SERVICES: [&str; 5] = [
    "build.bazel.remote.execution.v2.ActionCache",
    "build.bazel.remote.execution.v2.Capabilities",
    "build.bazel.remote.execution.v2.ContentAddressableStorage",
    "build.bazel.remote.execution.v2.Execution",
    "google.bytestream.ByteStream",
];

pub(super) type Headers = Arc<[(AsciiMetadataKey, AsciiMetadataValue)]>;

pub(super) struct CredentialHelper {
    program: PathBuf,
    /// Passed before `get`. Tests run their scripts through `sh` this way, since a script
    /// run right after it was written can fail with ETXTBSY on Linux while other tests fork.
    program_args: Vec<OsString>,
    timeout: Duration,
    services: BTreeMap<&'static str, ServiceHeaders>,
}

/// What the helper printed for one gRPC service's URI.
struct ServiceHeaders {
    uri: String,
    current: RwLock<CurrentHeaders>,
    /// Held while the helper runs, so that concurrent compiles run it once and wait for that run.
    running: Arc<tokio::sync::Mutex<()>>,
    /// When the helper may run again in the background, if it failed the last time it ran.
    retry_at: Mutex<Option<Instant>>,
}

struct CurrentHeaders {
    headers: Headers,
    renew_at: Instant,
    /// From then on, a compile waits for the helper before calling the server.
    expires_at: Instant,
    /// How many times the headers changed since sccache first ran the helper.
    version: u64,
}

/// When to run the helper again after it printed headers.
struct Lifetime {
    /// Until the helper runs again.
    renew_after: Duration,
    /// Until a compile waits for that run.
    expires_after: Duration,
}

#[derive(Deserialize)]
struct GetCredentialsResponse {
    #[serde(default)]
    headers: BTreeMap<String, Vec<String>>,
    expires: Option<String>,
}

impl CredentialHelper {
    /// A helper that has printed no headers yet. `server_uri` is `https://<host>[:<port>]`;
    /// each service's URI adds the service's name as the path.
    pub(super) fn new(program: PathBuf, server_uri: &str, timeout: Duration) -> Self {
        let services = SERVICES
            .into_iter()
            .map(|service| {
                let headers = ServiceHeaders {
                    uri: format!("{server_uri}/{service}"),
                    current: RwLock::new(CurrentHeaders {
                        headers: Arc::new([]),
                        renew_at: Instant::now(),
                        expires_at: Instant::now(),
                        version: 0,
                    }),
                    running: Default::default(),
                    retry_at: Mutex::new(None),
                };
                (service, headers)
            })
            .collect();
        CredentialHelper {
            program,
            program_args: Vec::new(),
            timeout,
            services,
        }
    }

    /// The headers the helper printed last for `service`, or `None` if sccache doesn't call
    /// `service`. Doesn't run the helper.
    pub(super) fn headers(&self, service: &str) -> Option<Headers> {
        let service = self.services.get(service)?;
        Some(service.current.read().unwrap().headers.clone())
    }

    /// How many times the headers changed, summed over the services, so that a server that
    /// refused some can be tried with the next.
    pub(super) fn version(&self) -> u64 {
        (self.services.values())
            .map(|service| service.current.read().unwrap().version)
            .sum()
    }

    /// Runs the helper again at the next [`renew`](Self::renew) or compile, which waits for it, as
    /// for headers the server refused.
    pub(super) fn expire(&self) {
        for service in self.services.values() {
            let mut current = service.current.write().unwrap();
            current.renew_at = Instant::now();
            current.expires_at = Instant::now();
        }
    }

    /// Runs the helper for the services whose headers are due to be renewed, at once, waiting
    /// for runs in progress to end first. A failed run keeps the headers the helper printed
    /// before for that service. Returns the first error.
    pub(super) async fn renew(&self) -> Result<()> {
        // Every run ends before an error is returned, so that one failure doesn't kill the
        // runs for the other services.
        let renewals = self.services.values().map(|service| async move {
            if !service.due() {
                return Ok(());
            }
            let _running = service.running.lock().await;
            self.renew_while_running(service).await
        });
        futures::future::join_all(renewals)
            .await
            .into_iter()
            .collect()
    }

    /// Waits for the runs [`renew_in_background`](Self::renew_in_background) started for the
    /// services whose headers expired, since headers past `expires` aren't valid for new calls.
    pub(super) async fn wait_for_expired(&self) {
        let waits = self.services.values().map(|service| async move {
            if service.expired() {
                drop(service.running.lock().await);
            }
        });
        futures::future::join_all(waits).await;
    }

    /// Starts [`renew`](Self::renew) for each service whose headers are due, if no run for it
    /// is in progress and the helper didn't fail for it in the last [`RETRY_AFTER_FAILURE`].
    /// The first failure in a row is logged.
    pub(super) fn renew_in_background(self: &Arc<Self>) -> Vec<JoinHandle<()>> {
        (self.services.iter())
            .filter_map(|(&name, service)| {
                if !service.due() {
                    return None;
                }
                let running = service.running.clone().try_lock_owned().ok()?;
                // A run that failed sets `retry_at` before it releases `running`.
                if service.failed_recently() {
                    return None;
                }
                let failed_before = service.retry_at.lock().unwrap().is_some();
                let this = self.clone();
                Some(tokio::spawn(async move {
                    let renewed = this.renew_while_running(&this.services[name]).await;
                    drop(running);
                    if let Err(e) = renewed
                        && !failed_before
                    {
                        warn!(
                            "REAPI: {e:#}; keeping the headers the helper printed before for {name}"
                        );
                    }
                }))
            })
            .collect()
    }

    /// [`renew`](Self::renew) for `service`, with its `running` held.
    async fn renew_while_running(&self, service: &ServiceHeaders) -> Result<()> {
        if !service.due() {
            return Ok(());
        }
        let (headers, lifetime) = self.run(&service.uri).await.inspect_err(|_| {
            *service.retry_at.lock().unwrap() = Some(Instant::now() + RETRY_AFTER_FAILURE);
        })?;
        *service.retry_at.lock().unwrap() = None;
        let mut current = service.current.write().unwrap();
        if headers[..] != current.headers[..] {
            current.headers = headers.into();
            current.version += 1;
        }
        let now = Instant::now();
        current.renew_at = now + lifetime.renew_after;
        current.expires_at = now + lifetime.expires_after;
        Ok(())
    }

    /// The headers the helper prints for `uri`, and how long to use them.
    async fn run(
        &self,
        uri: &str,
    ) -> Result<(Vec<(AsciiMetadataKey, AsciiMetadataValue)>, Lifetime)> {
        let program = self.program.display();
        // The URI holds the server's host, which logs leave out.
        let service = uri.rsplit('/').next().unwrap_or_default();
        debug!("REAPI: running the credential helper {program} for {service}");
        let mut child = tokio::process::Command::new(&self.program)
            .args(&self.program_args)
            .arg("get")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("failed to run the REAPI credential helper {program}"))?;
        let request = serde_json::json!({ "uri": uri }).to_string();
        let mut stdin = child.stdin.take().unwrap();
        let mut stdout = child.stdout.take().unwrap().take(MAX_OUTPUT_BYTES + 1);
        let (status, output) = tokio::time::timeout(self.timeout, async {
            // A helper may exit without reading its stdin.
            let _ = stdin.write_all(request.as_bytes()).await;
            drop(stdin);
            let mut output = Vec::new();
            stdout.read_to_end(&mut output).await?;
            // A helper that prints more may wait for its output to be read, so it's killed
            // instead of waited for.
            if output.len() as u64 > MAX_OUTPUT_BYTES {
                return Ok(None);
            }
            Ok::<_, std::io::Error>(Some((child.wait().await?, output)))
        })
        .await
        .with_context(|| {
            format!(
                "the REAPI credential helper {program} didn't finish within {} seconds",
                self.timeout.as_secs()
            )
        })?
        .with_context(|| format!("failed to run the REAPI credential helper {program}"))?
        .with_context(|| {
            format!(
                "the REAPI credential helper {program} printed more than {MAX_OUTPUT_BYTES} bytes"
            )
        })?;
        if !status.success() {
            bail!("the REAPI credential helper {program} failed: {status}");
        }
        parse_response(&output, Utc::now())
            .with_context(|| format!("the REAPI credential helper {program} printed bad output"))
    }
}

impl ServiceHeaders {
    fn due(&self) -> bool {
        Instant::now() >= self.current.read().unwrap().renew_at
    }

    fn expired(&self) -> bool {
        Instant::now() >= self.current.read().unwrap().expires_at
    }

    fn failed_recently(&self) -> bool {
        (self.retry_at.lock().unwrap()).is_some_and(|at| Instant::now() < at)
    }
}

/// The headers in a helper's `output`, and how long to use them from `now`. Errors never
/// include a header's value.
fn parse_response(
    output: &[u8],
    now: DateTime<Utc>,
) -> Result<(Vec<(AsciiMetadataKey, AsciiMetadataValue)>, Lifetime)> {
    // serde_json's errors may quote the output, which holds credentials.
    let response: GetCredentialsResponse = serde_json::from_slice(output).map_err(|e| {
        anyhow!(
            "not the JSON of a GetCredentialsResponse, at line {} column {}",
            e.line(),
            e.column()
        )
    })?;
    let mut headers = Vec::new();
    for (name, values) in &response.headers {
        let key = header_key(name)?;
        for value in values {
            let value = sensitive_ascii_value(value).with_context(|| {
                format!("the header {name:?} has a non-ASCII or control character")
            })?;
            headers.push((key.clone(), value));
        }
    }
    let lifetime = match response.expires {
        Some(expires) => {
            let expires = DateTime::parse_from_rfc3339(&expires)
                .map_err(|_| anyhow!("`expires` isn't an RFC 3339 time"))?;
            let expires_after = (expires.with_timezone(&Utc) - now)
                .to_std()
                .unwrap_or_default()
                .max(MIN_LIFETIME);
            Lifetime {
                renew_after: expires_after
                    .saturating_sub(RENEW_BEFORE_EXPIRY)
                    .max(MIN_LIFETIME),
                expires_after,
            }
        }
        None => Lifetime {
            renew_after: LIFETIME_WITHOUT_EXPIRY,
            expires_after: LIFETIME_WITHOUT_EXPIRY,
        },
    };
    Ok((headers, lifetime))
}

#[cfg(test)]
pub(super) mod test {
    use super::*;
    use test_case::test_case;

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-10-30T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn header_strings(headers: &[(AsciiMetadataKey, AsciiMetadataValue)]) -> Vec<(String, String)> {
        headers
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_str().unwrap().to_owned()))
            .collect()
    }

    #[test]
    fn test_headers_are_read_with_every_value_and_left_out_of_logs() {
        let (headers, _) = parse_response(
            br#"{"headers": {"Authorization": ["Bearer secret"], "x-tenant": ["a", "b"]}}"#,
            now(),
        )
        .unwrap();
        assert_eq!(
            vec![
                ("authorization".to_owned(), "Bearer secret".to_owned()),
                ("x-tenant".to_owned(), "a".to_owned()),
                ("x-tenant".to_owned(), "b".to_owned()),
            ],
            header_strings(&headers)
        );
        assert!(headers.iter().all(|(_, value)| value.is_sensitive()));
    }

    #[test]
    fn test_other_properties_and_empty_value_lists_are_ignored() {
        let (headers, _) = parse_response(
            br#"{"headers": {"x": ["a"], "y": []}, "z": {"w": 1}}"#,
            now(),
        )
        .unwrap();
        assert_eq!(
            vec![("x".to_owned(), "a".to_owned())],
            header_strings(&headers)
        );
    }

    #[test_case(
        br#"{}"#,
        LIFETIME_WITHOUT_EXPIRY,
        LIFETIME_WITHOUT_EXPIRY ;
        "no expires"
    )]
    #[test_case(
        br#"{"expires": "2026-10-30T13:00:00+01:00"}"#,
        MIN_LIFETIME,
        MIN_LIFETIME ;
        "expires now in another time zone"
    )]
    #[test_case(
        br#"{"expires": "2026-10-30T12:10:00Z"}"#,
        Duration::from_secs(9 * 60),
        Duration::from_secs(10 * 60) ;
        "renewed a minute before expiry"
    )]
    #[test_case(
        br#"{"expires": "2026-10-30T12:00:30Z"}"#,
        MIN_LIFETIME,
        Duration::from_secs(30) ;
        "expires within a minute"
    )]
    #[test_case(
        br#"{"expires": "2026-10-30T11:00:00Z"}"#,
        MIN_LIFETIME,
        MIN_LIFETIME ;
        "expired"
    )]
    fn test_headers_are_renewed_shortly_before_they_expire(
        output: &[u8],
        renew_after: Duration,
        expires_after: Duration,
    ) {
        let (_, lifetime) = parse_response(output, now()).unwrap();
        assert_eq!(
            (renew_after, expires_after),
            (lifetime.renew_after, lifetime.expires_after)
        );
    }

    #[test_case(br#"{"headers": {"x": "secret"}}"# ; "a value that isn't a list")]
    #[test_case(br#"{"headers": {"x": ["sec\nret"]}}"# ; "control character")]
    #[test_case(br#"{"headers": {"te": ["secret"]}}"# ; "reserved name")]
    #[test_case(br#"{"headers": {"x": ["secret"]}, "expires": "secret"}"# ; "bad expires")]
    #[test_case(br#"secret"# ; "not json")]
    fn test_bad_output_is_refused_without_echoing_it(output: &[u8]) {
        let e = parse_response(output, now()).err().unwrap();
        assert!(!format!("{e:#}").contains("ret"), "{e:#}");
    }

    /// A helper that runs `body`, through `sh`, from a script in `dir`.
    #[cfg(unix)]
    fn sh_helper(dir: &std::path::Path, body: &str, timeout: Duration) -> CredentialHelper {
        let script = dir.join("helper");
        std::fs::write(&script, body).unwrap();
        CredentialHelper {
            program_args: vec![script.into()],
            ..CredentialHelper::new("/bin/sh".into(), "https://farm", timeout)
        }
    }

    /// A script that writes the request it got to `dir`'s `runs`, checks that it got `get` and
    /// a URI under `https://farm/`, and prints `output`.
    #[cfg(unix)]
    pub(in crate::reapi) fn script_body(dir: &std::path::Path, output: &str) -> String {
        format!(
            "request=$(cat)\necho \"$request\" >> '{}'\n[ \"$1\" = get ] || exit 2\n\
             case \"$request\" in *'\"uri\":\"https://farm/'*) ;; *) exit 3 ;; esac\n\
             printf '%s' '{output}'\n",
            dir.join("runs").display()
        )
    }

    /// A helper in `dir` that prints `output` for every service; [`runs`] counts its runs, one
    /// per service each time its headers are renewed.
    #[cfg(unix)]
    pub(in crate::reapi) fn script_helper(dir: &std::path::Path, output: &str) -> CredentialHelper {
        sh_helper(dir, &script_body(dir, output), HELPER_TIMEOUT)
    }

    /// A helper in `dir` that prints the URI it got as the header `x-uri`.
    #[cfg(unix)]
    pub(in crate::reapi) fn uri_helper(dir: &std::path::Path) -> CredentialHelper {
        let body = "uri=$(sed 's/.*\"uri\":\"\\([^\"]*\\)\".*/\\1/')\n\
                    printf '{\"headers\": {\"x-uri\": [\"%s\"]}}' \"$uri\"\n";
        sh_helper(dir, body, HELPER_TIMEOUT)
    }

    /// What a connected compile does with the helper before it calls the server.
    #[cfg(unix)]
    async fn start_compile(helper: &Arc<CredentialHelper>) -> Vec<JoinHandle<()>> {
        let renewals = helper.renew_in_background();
        helper.wait_for_expired().await;
        renewals
    }

    #[cfg(unix)]
    pub(in crate::reapi) fn runs(dir: &std::path::Path) -> usize {
        std::fs::read_to_string(dir.join("runs")).map_or(0, |runs| runs.lines().count())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_the_helper_runs_once_until_its_headers_are_due() {
        let dir = tempfile::tempdir().unwrap();
        let helper = Arc::new(script_helper(
            dir.path(),
            r#"{"headers": {"x": ["secret"]}}"#,
        ));
        let renewals: Vec<_> = (0..4)
            .map(|_| {
                let helper = helper.clone();
                tokio::spawn(async move { helper.renew().await.is_ok() })
            })
            .collect();
        for renewal in renewals {
            assert!(renewal.await.unwrap());
        }
        assert_eq!(SERVICES.len(), runs(dir.path()));
        assert_eq!(SERVICES.len() as u64, helper.version());
        for service in SERVICES {
            assert_eq!(
                vec![("x".to_owned(), "secret".to_owned())],
                header_strings(&helper.headers(service).unwrap())
            );
        }
        helper.expire();
        helper.renew().await.unwrap();
        assert_eq!(2 * SERVICES.len(), runs(dir.path()));
        assert_eq!(
            SERVICES.len() as u64,
            helper.version(),
            "the same headers again"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_each_service_gets_its_uri_and_the_headers_printed_for_it() {
        let dir = tempfile::tempdir().unwrap();
        let helper = uri_helper(dir.path());
        helper.renew().await.unwrap();
        for service in SERVICES {
            assert_eq!(
                vec![("x-uri".to_owned(), format!("https://farm/{service}"))],
                header_strings(&helper.headers(service).unwrap())
            );
        }
        assert!(helper.headers("google.longrunning.Operations").is_none());
    }

    #[test]
    fn test_the_services_are_the_ones_sccache_has_clients_for() {
        use crate::reapi::proto::build::bazel::remote::execution::v2::{
            action_cache_server, capabilities_server, content_addressable_storage_server,
            execution_server,
        };
        use crate::reapi::proto::google::bytestream::byte_stream_server;
        let mut services = [
            action_cache_server::SERVICE_NAME,
            capabilities_server::SERVICE_NAME,
            content_addressable_storage_server::SERVICE_NAME,
            execution_server::SERVICE_NAME,
            byte_stream_server::SERVICE_NAME,
        ];
        services.sort();
        assert_eq!(SERVICES, services);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_a_compile_waits_only_for_expired_headers() {
        let dir = tempfile::tempdir().unwrap();
        let helper = Arc::new(script_helper(
            dir.path(),
            r#"{"headers": {"x": ["secret"]}}"#,
        ));
        helper.renew().await.unwrap();
        for service in helper.services.values() {
            service.current.write().unwrap().renew_at = Instant::now();
        }
        let renewals = start_compile(&helper).await;
        assert_eq!(SERVICES.len(), runs(dir.path()), "due but not expired");
        for renewal in renewals {
            renewal.await.unwrap();
        }
        assert_eq!(2 * SERVICES.len(), runs(dir.path()));
        helper.expire();
        *helper.services[SERVICES[0]].retry_at.lock().unwrap() =
            Some(Instant::now() + Duration::from_secs(60));
        *helper.services[SERVICES[1]].retry_at.lock().unwrap() = Some(Instant::now());
        start_compile(&helper).await;
        assert_eq!(
            3 * SERVICES.len() - 1,
            runs(dir.path()),
            "only the one that failed recently isn't waited for"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_a_hanging_helper_runs_once_for_the_compiles_waiting_for_it() {
        let dir = tempfile::tempdir().unwrap();
        let body = script_body(dir.path(), "{}").replace("printf", "sleep 5\nprintf");
        let helper = Arc::new(sh_helper(dir.path(), &body, Duration::from_secs(1)));
        let before = Instant::now();
        let compiles: Vec<_> = (0..4)
            .map(|_| {
                let helper = helper.clone();
                tokio::spawn(async move { start_compile(&helper).await })
            })
            .collect();
        for compile in compiles {
            compile.await.unwrap();
        }
        assert_eq!(SERVICES.len(), runs(dir.path()));
        assert!(
            before.elapsed() < Duration::from_secs(3),
            "{:?}",
            before.elapsed()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_a_helper_that_failed_in_the_background_waits_before_running_again() {
        let dir = tempfile::tempdir().unwrap();
        let helper = Arc::new(script_helper(dir.path(), "not json"));
        let renewals = helper.renew_in_background();
        assert_eq!(SERVICES.len(), renewals.len());
        assert!(
            helper.renew_in_background().is_empty(),
            "runs are in progress"
        );
        for renewal in renewals {
            renewal.await.unwrap();
        }
        assert!(helper.renew_in_background().is_empty(), "they just failed");
        *helper.services[SERVICES[0]].retry_at.lock().unwrap() = Some(Instant::now());
        for renewal in helper.renew_in_background() {
            renewal.await.unwrap();
        }
        assert_eq!(SERVICES.len() + 1, runs(dir.path()));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_a_helper_that_prints_too_much_is_stopped() {
        let dir = tempfile::tempdir().unwrap();
        let helper = sh_helper(
            dir.path(),
            "while :; do printf '                '; done\n",
            HELPER_TIMEOUT,
        );
        let e = helper.renew().await.err().unwrap();
        assert!(format!("{e:#}").contains("printed more than"), "{e:#}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_a_helper_that_closes_its_stdin_works() {
        let dir = tempfile::tempdir().unwrap();
        let helper = sh_helper(
            dir.path(),
            "exec 0<&-\nprintf '{\"headers\": {\"x\": [\"a\"]}}'\n",
            HELPER_TIMEOUT,
        );
        helper.renew().await.unwrap();
        assert_eq!(1, helper.headers(SERVICES[0]).unwrap().len());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_a_helper_that_isnt_executable_fails() {
        let dir = tempfile::tempdir().unwrap();
        let program = dir.path().join("helper");
        std::fs::write(&program, "printf '{}'\n").unwrap();
        let helper = CredentialHelper::new(program, "https://farm", HELPER_TIMEOUT);
        let e = helper.renew().await.err().unwrap();
        assert!(format!("{e:#}").contains("failed to run"), "{e:#}");
    }

    #[cfg(unix)]
    #[test_case("exit 1" ; "failing")]
    #[test_case("sleep 5" ; "slow")]
    #[tokio::test]
    async fn test_a_helper_that_fails_or_hangs_keeps_the_last_headers(failure: &str) {
        let dir = tempfile::tempdir().unwrap();
        let output = r#"{"headers": {"x": ["secret"]}}"#;
        let helper = sh_helper(
            dir.path(),
            &script_body(dir.path(), output),
            Duration::from_secs(1),
        );
        helper.renew().await.unwrap();
        let body = script_body(dir.path(), output).replace("printf", &format!("{failure}\nprintf"));
        std::fs::write(dir.path().join("helper"), body).unwrap();
        helper.expire();
        assert!(helper.renew().await.is_err());
        assert_eq!(
            2 * SERVICES.len(),
            runs(dir.path()),
            "the failure isn't the script's"
        );
        assert_eq!(SERVICES.len() as u64, helper.version());
        for service in SERVICES {
            assert_eq!(1, helper.headers(service).unwrap().len());
        }
    }
}
