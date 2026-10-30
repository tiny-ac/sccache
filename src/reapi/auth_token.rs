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

//! The bearer token sccache sends to a REAPI server. Its file is read again at most every
//! [`CHECK_INTERVAL`] while compiles use the server, since a scheduled job may replace
//! short-lived tokens there.

use super::client::sensitive_ascii_value;
use crate::errors::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tonic::metadata::AsciiMetadataValue;

/// How often `auth_token_file` is read again at most.
pub(super) const CHECK_INTERVAL: Duration = Duration::from_secs(10);

pub(super) struct AuthToken {
    /// `None` for a token that never changes.
    file: Option<TokenFile>,
    current: RwLock<CurrentToken>,
    last_read: Mutex<Instant>,
    /// Set while a read of the file runs, so that a file that is slow to read starts no
    /// second read.
    reading: AtomicBool,
    /// Whether the last read of the file failed, so that a failure is logged once.
    read_failed: AtomicBool,
}

struct TokenFile {
    path: PathBuf,
    check_interval: Duration,
}

struct CurrentToken {
    header: AsciiMetadataValue,
    /// How many times the token changed since sccache first read it.
    version: u64,
}

impl AuthToken {
    /// The token in `file`, read again at most every `check_interval`. Fails if the file
    /// can't be read, or holds no valid token.
    pub(super) fn from_file(path: PathBuf, check_interval: Duration) -> Result<Self> {
        let header = bearer_header(&read(&path)?)?;
        Ok(Self::new(
            Some(TokenFile {
                path,
                check_interval,
            }),
            header,
        ))
    }

    /// A token that never changes.
    #[cfg(test)]
    pub(super) fn fixed(token: &str) -> Result<Self> {
        Ok(Self::new(None, bearer_header(token)?))
    }

    fn new(file: Option<TokenFile>, header: AsciiMetadataValue) -> Self {
        AuthToken {
            file,
            current: RwLock::new(CurrentToken { header, version: 0 }),
            last_read: Mutex::new(Instant::now()),
            reading: AtomicBool::new(false),
            read_failed: AtomicBool::new(false),
        }
    }

    /// The `authorization` header for the token. Doesn't read the file.
    pub(super) fn header(&self) -> AsciiMetadataValue {
        self.current.read().unwrap().header.clone()
    }

    /// How many times the token changed, so that a server that refused one token can be
    /// tried with the next.
    pub(super) fn version(&self) -> u64 {
        self.current.read().unwrap().version
    }

    /// Starts reading the file again on a blocking thread, if it wasn't read for the check
    /// interval and no read is running. A file that can't be read, or holds no valid token,
    /// leaves the token as it was.
    pub(super) fn read_again(self: &Arc<Self>) -> Option<JoinHandle<()>> {
        let file = self.file.as_ref()?;
        {
            let mut last_read = self.last_read.lock().unwrap();
            if last_read.elapsed() < file.check_interval
                || self.reading.swap(true, Ordering::SeqCst)
            {
                return None;
            }
            *last_read = Instant::now();
        }
        let path = file.path.clone();
        let this = self.clone();
        Some(tokio::spawn(async move {
            let header = tokio::task::spawn_blocking({
                let path = path.clone();
                move || bearer_header(&read(&path)?)
            })
            .await
            .map_err(Error::from)
            .flatten();
            this.reading.store(false, Ordering::SeqCst);
            match header {
                Ok(header) => {
                    this.read_failed.store(false, Ordering::SeqCst);
                    let changed = {
                        let mut current = this.current.write().unwrap();
                        let changed = header != current.header;
                        if changed {
                            current.header = header;
                            current.version += 1;
                        }
                        changed
                    };
                    if changed {
                        info!("REAPI: using the new auth token in {}", path.display());
                    }
                }
                Err(e) => {
                    if !this.read_failed.swap(true, Ordering::SeqCst) {
                        warn!("REAPI: {e:#}; keeping the auth token sccache read before");
                    }
                }
            }
        }))
    }
}

fn read(path: &Path) -> Result<String> {
    std::fs::read_to_string(path).with_context(|| {
        format!(
            "failed to read the REAPI auth token from {}",
            path.display()
        )
    })
}

/// The bearer `authorization` header for `token`, without its surrounding whitespace.
fn bearer_header(token: &str) -> Result<AsciiMetadataValue> {
    let token = token.trim();
    if token.is_empty() {
        bail!("the REAPI auth token is empty");
    }
    sensitive_ascii_value(&format!("Bearer {token}"))
        .context("the REAPI auth token contains a non-ASCII or control character")
}

#[cfg(test)]
pub(super) mod test {
    use super::*;
    use test_case::test_case;

    #[test_case(" \n" ; "empty")]
    #[test_case("sec\nret" ; "control character")]
    #[test_case("sécret" ; "non-ascii")]
    fn test_an_invalid_token_is_refused_without_echoing_it(token: &str) {
        let e = AuthToken::fixed(token).err().unwrap();
        assert!(!format!("{e:#}").contains("ret"), "{e:#}");
    }

    #[test]
    fn test_the_token_is_sent_without_surrounding_whitespace_and_left_out_of_logs() {
        let header = AuthToken::fixed(" secret\n").unwrap().header();
        assert_eq!("Bearer secret", header);
        assert!(header.is_sensitive());
    }

    /// A token in a file of `dir` that is read again at most every `check_interval`, and a
    /// function that writes a new token there.
    pub(in crate::reapi) fn token_file(
        dir: &Path,
        check_interval: Duration,
    ) -> (Arc<AuthToken>, impl Fn(&str)) {
        let file = dir.join("token");
        std::fs::write(&file, "first").unwrap();
        let auth_token = Arc::new(AuthToken::from_file(file.clone(), check_interval).unwrap());
        (auth_token, move |token: &str| {
            std::fs::write(&file, token).unwrap();
        })
    }

    /// `auth_token`'s header and version after it reads its file again, once a read that
    /// already runs ends.
    pub(in crate::reapi) async fn read_again_now(auth_token: &Arc<AuthToken>) -> (String, u64) {
        let read = async {
            loop {
                match auth_token.read_again() {
                    Some(read) => break read.await.unwrap(),
                    None => tokio::task::yield_now().await,
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(10), read)
            .await
            .expect("the token file wasn't read again");
        (
            auth_token.header().to_str().unwrap().to_owned(),
            auth_token.version(),
        )
    }

    #[tokio::test]
    async fn test_a_changed_file_is_read_again() {
        let dir = tempfile::tempdir().unwrap();
        let (auth_token, write_token) = token_file(dir.path(), Duration::ZERO);
        write_token("first\n");
        assert_eq!(
            ("Bearer first".to_owned(), 0),
            read_again_now(&auth_token).await,
            "only whitespace changed"
        );
        write_token("second\n");
        assert_eq!(
            ("Bearer second".to_owned(), 1),
            read_again_now(&auth_token).await
        );
    }

    #[tokio::test]
    async fn test_the_file_is_read_at_most_every_check_interval() {
        let interval = Duration::from_secs(3600);
        let dir = tempfile::tempdir().unwrap();
        let (auth_token, write_token) = token_file(dir.path(), interval);
        write_token("second");
        assert!(auth_token.read_again().is_none());
        assert_eq!("Bearer first", auth_token.header());
        *auth_token.last_read.lock().unwrap() = Instant::now() - interval;
        auth_token.read_again().unwrap().await.unwrap();
        assert_eq!("Bearer second", auth_token.header());
        assert!(auth_token.read_again().is_none());
    }

    #[tokio::test]
    async fn test_a_read_starts_no_second_one() {
        let dir = tempfile::tempdir().unwrap();
        let (auth_token, _) = token_file(dir.path(), Duration::ZERO);
        let read = auth_token.read_again().unwrap();
        assert!(auth_token.read_again().is_none());
        read.await.unwrap();
        assert!(auth_token.read_again().is_some());
    }

    #[test_case(None ; "removed")]
    #[test_case(Some(" \n") ; "emptied")]
    #[tokio::test]
    async fn test_a_file_that_no_longer_holds_a_token_keeps_the_last_one(contents: Option<&str>) {
        let dir = tempfile::tempdir().unwrap();
        let (auth_token, write_token) = token_file(dir.path(), Duration::ZERO);
        match contents {
            Some(contents) => write_token(contents),
            None => std::fs::remove_file(dir.path().join("token")).unwrap(),
        }
        assert_eq!(
            ("Bearer first".to_owned(), 0),
            read_again_now(&auth_token).await
        );
    }
}
