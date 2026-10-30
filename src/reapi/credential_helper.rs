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
//! (<https://github.com/EngFlow/credential-helper-spec>): sccache runs `<helper> get`, writes
//! `{"uri": "https://<host>[:<port>]"}` to its stdin, and sends the headers it prints with
//! every call, running it again before they expire.

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
/// How long after a failed run the helper isn't run again in the background, so that a
/// failing helper doesn't run for every compile.
const RETRY_AFTER_FAILURE: Duration = Duration::from_secs(10);
/// More than any helper prints, so that a broken one can't fill the memory.
const MAX_OUTPUT_BYTES: u64 = 1 << 20;

pub(super) type Headers = Arc<[(AsciiMetadataKey, AsciiMetadataValue)]>;

pub(super) struct CredentialHelper {
    program: PathBuf,
    /// Passed before `get`. Tests run their scripts through `sh` this way, since a script
    /// run right after it was written can fail with ETXTBSY on Linux while other tests fork.
    program_args: Vec<OsString>,
    uri: String,
    timeout: Duration,
    current: RwLock<CurrentHeaders>,
    /// Held while the helper runs, so that concurrent compiles run it once.
    running: Arc<tokio::sync::Mutex<()>>,
    /// When the helper failed, if it failed the last time it ran.
    failed_at: Mutex<Option<Instant>>,
}

struct CurrentHeaders {
    headers: Headers,
    renew_at: Instant,
    /// How many times the headers changed since sccache first ran the helper.
    version: u64,
}

#[derive(Deserialize)]
struct GetCredentialsResponse {
    #[serde(default)]
    headers: BTreeMap<String, Vec<String>>,
    expires: Option<String>,
}

impl CredentialHelper {
    /// A helper that has printed no headers yet. `uri` is what it gets on its stdin.
    pub(super) fn new(program: PathBuf, uri: String, timeout: Duration) -> Self {
        CredentialHelper {
            program,
            program_args: Vec::new(),
            uri,
            timeout,
            current: RwLock::new(CurrentHeaders {
                headers: Arc::new([]),
                renew_at: Instant::now(),
                version: 0,
            }),
            running: Default::default(),
            failed_at: Mutex::new(None),
        }
    }

    /// The headers the helper printed last. Doesn't run it.
    pub(super) fn headers(&self) -> Headers {
        self.current.read().unwrap().headers.clone()
    }

    /// How many times the headers changed, so that a server that refused some can be tried
    /// with the next.
    pub(super) fn version(&self) -> u64 {
        self.current.read().unwrap().version
    }

    /// Runs the helper again at the next [`renew`](Self::renew), as for headers the server
    /// refused.
    pub(super) fn expire(&self) {
        self.current.write().unwrap().renew_at = Instant::now();
    }

    /// Runs the helper if its headers are due to be renewed, waiting for a run in progress to
    /// end first. A failed run keeps the headers the helper printed before.
    pub(super) async fn renew(&self) -> Result<()> {
        if !self.due() {
            return Ok(());
        }
        let _running = self.running.lock().await;
        self.renew_while_running().await
    }

    /// Starts [`renew`](Self::renew), if the headers are due, no run is in progress, and the
    /// helper didn't fail in the last [`RETRY_AFTER_FAILURE`]. The first failure in a row is
    /// logged.
    pub(super) fn renew_in_background(self: &Arc<Self>) -> Option<JoinHandle<()>> {
        let failed_at = *self.failed_at.lock().unwrap();
        if !self.due() || failed_at.is_some_and(|at| at.elapsed() < RETRY_AFTER_FAILURE) {
            return None;
        }
        let running = self.running.clone().try_lock_owned().ok()?;
        let this = self.clone();
        Some(tokio::spawn(async move {
            let renewed = this.renew_while_running().await;
            drop(running);
            if let Err(e) = renewed
                && failed_at.is_none()
            {
                warn!("REAPI: {e:#}; keeping the headers it printed before");
            }
        }))
    }

    /// [`renew`](Self::renew), with `running` held.
    async fn renew_while_running(&self) -> Result<()> {
        if !self.due() {
            return Ok(());
        }
        let (headers, lifetime) = self.run().await.inspect_err(|_| {
            *self.failed_at.lock().unwrap() = Some(Instant::now());
        })?;
        *self.failed_at.lock().unwrap() = None;
        let mut current = self.current.write().unwrap();
        if headers[..] != current.headers[..] {
            current.headers = headers.into();
            current.version += 1;
        }
        current.renew_at = Instant::now() + lifetime;
        Ok(())
    }

    fn due(&self) -> bool {
        Instant::now() >= self.current.read().unwrap().renew_at
    }

    /// The headers the helper prints, and how long to use them.
    async fn run(&self) -> Result<(Vec<(AsciiMetadataKey, AsciiMetadataValue)>, Duration)> {
        let program = self.program.display();
        let mut child = tokio::process::Command::new(&self.program)
            .args(&self.program_args)
            .arg("get")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("failed to run the REAPI credential helper {program}"))?;
        let request = serde_json::json!({ "uri": self.uri }).to_string();
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

/// The headers in a helper's `output`, and how long to use them from `now`. Errors never
/// include a header's value.
fn parse_response(
    output: &[u8],
    now: DateTime<Utc>,
) -> Result<(Vec<(AsciiMetadataKey, AsciiMetadataValue)>, Duration)> {
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
            (expires.with_timezone(&Utc) - now)
                .to_std()
                .unwrap_or_default()
                .saturating_sub(RENEW_BEFORE_EXPIRY)
        }
        None => LIFETIME_WITHOUT_EXPIRY,
    };
    Ok((headers, lifetime.max(MIN_LIFETIME)))
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

    #[test_case(br#"{}"#, LIFETIME_WITHOUT_EXPIRY ; "no expires")]
    #[test_case(
        br#"{"expires": "2026-10-30T13:00:00+01:00"}"#,
        MIN_LIFETIME ;
        "expires now in another time zone"
    )]
    #[test_case(
        br#"{"expires": "2026-10-30T12:10:00Z"}"#,
        Duration::from_secs(9 * 60) ;
        "renewed a minute before expiry"
    )]
    #[test_case(br#"{"expires": "2026-10-30T11:00:00Z"}"#, MIN_LIFETIME ; "expired")]
    fn test_headers_are_used_until_shortly_before_they_expire(output: &[u8], lifetime: Duration) {
        assert_eq!(lifetime, parse_response(output, now()).unwrap().1);
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
            ..CredentialHelper::new("/bin/sh".into(), "https://farm".to_owned(), timeout)
        }
    }

    /// A script that counts its runs in `dir`, checks that it got `get` and the URI, and
    /// prints `output`.
    #[cfg(unix)]
    fn script_body(dir: &std::path::Path, output: &str) -> String {
        format!(
            "echo run >> '{}'\n[ \"$1\" = get ] || exit 2\n\
             grep -q '\"uri\":\"https://farm\"' || exit 3\nprintf '%s' '{output}'\n",
            dir.join("runs").display()
        )
    }

    /// A helper in `dir` that prints `output`; [`runs`] counts its runs.
    #[cfg(unix)]
    pub(in crate::reapi) fn script_helper(dir: &std::path::Path, output: &str) -> CredentialHelper {
        sh_helper(dir, &script_body(dir, output), HELPER_TIMEOUT)
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
        assert_eq!(1, runs(dir.path()));
        assert_eq!(1, helper.version());
        assert_eq!(
            vec![("x".to_owned(), "secret".to_owned())],
            header_strings(&helper.headers())
        );
        helper.expire();
        helper.renew().await.unwrap();
        assert_eq!(2, runs(dir.path()));
        assert_eq!(1, helper.version(), "the same headers again");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_a_helper_that_failed_in_the_background_waits_before_running_again() {
        let dir = tempfile::tempdir().unwrap();
        let helper = Arc::new(script_helper(dir.path(), "not json"));
        let renewal = helper.renew_in_background().unwrap();
        assert!(
            helper.renew_in_background().is_none(),
            "a run is in progress"
        );
        renewal.await.unwrap();
        assert!(helper.renew_in_background().is_none(), "it just failed");
        *helper.failed_at.lock().unwrap() = Some(Instant::now() - RETRY_AFTER_FAILURE);
        helper.renew_in_background().unwrap().await.unwrap();
        assert_eq!(2, runs(dir.path()));
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
        assert_eq!(2, runs(dir.path()), "the failure isn't the script's");
        assert_eq!(1, helper.version());
        assert_eq!(1, helper.headers().len());
    }
}
