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

//! The toolchains whose files go into a compile's input root. The first compile that needs a
//! toolchain starts hashing its files in a separate task, and no compile waits for that task
//! past its deadline. The digest cache keeps the digests across sccache server restarts.

use super::digest::DigestCache;
use super::input_root::InputRootFile;
use super::mapping::{CompileLocallyReason, ToolchainKey};
use super::remote_compile::{UnusableToolchain, toolchain_files};
use crate::errors::*;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;

const RETRY_AFTER: Duration = Duration::from_secs(60);

/// A toolchain's input root files, paired with their local paths.
pub(super) type ToolchainFiles = Arc<Vec<(InputRootFile, PathBuf)>>;

enum State {
    Hashing,
    Hashed(ToolchainFiles),
    HashingFailed {
        retry_at: Instant,
    },
    /// Compiles with the toolchain run locally until it changes, which changes its key.
    Unusable(CompileLocallyReason),
}

pub(super) struct Toolchains {
    digest_cache: Arc<DigestCache>,
    states: tokio::sync::Mutex<HashMap<ToolchainKey, State>>,
    hash_attempts_ended: watch::Sender<u64>,
    retry_after: Duration,
}

impl Toolchains {
    pub(super) fn new(digest_cache: DigestCache) -> Arc<Self> {
        Arc::new(Toolchains {
            digest_cache: Arc::new(digest_cache),
            states: Default::default(),
            hash_attempts_ended: watch::Sender::new(0),
            retry_after: RETRY_AFTER,
        })
    }

    /// `key`'s files, or why a compile runs locally instead. Waits for them to be hashed up
    /// to `deadline`.
    pub(super) async fn files(
        self: &Arc<Self>,
        key: &ToolchainKey,
        deadline: Instant,
    ) -> std::result::Result<ToolchainFiles, CompileLocallyReason> {
        let mut hash_attempts_ended = self.hash_attempts_ended.subscribe();
        let mut waited = false;
        loop {
            {
                let mut states = self.states.lock().await;
                match states.get(key) {
                    Some(State::Hashed(files)) => return Ok(files.clone()),
                    Some(State::Unusable(reason)) => return Err(*reason),
                    // A compile that waited for a hashing that failed doesn't start another.
                    Some(State::HashingFailed { retry_at })
                        if waited || Instant::now() < *retry_at =>
                    {
                        return Err(CompileLocallyReason::HashingToolchainFailed);
                    }
                    Some(State::Hashing) => {}
                    None | Some(State::HashingFailed { .. }) => {
                        states.insert(key.clone(), State::Hashing);
                        self.spawn_hash(key.clone());
                    }
                }
            }
            waited = true;
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero()
                || tokio::time::timeout(remaining, hash_attempts_ended.changed())
                    .await
                    .is_err()
            {
                return Err(CompileLocallyReason::HashingToolchain);
            }
        }
    }

    fn spawn_hash(self: &Arc<Self>, key: ToolchainKey) {
        let this = self.clone();
        tokio::spawn(async move {
            let (sysroot, target_triple, digest_cache) = (
                key.sysroot.clone(),
                key.target_triple.clone(),
                this.digest_cache.clone(),
            );
            let hashed = tokio::task::spawn_blocking(move || {
                let files = toolchain_files(&sysroot, &target_triple, &digest_cache)?;
                if let Err(e) = digest_cache.save() {
                    warn!("REAPI: failed to save the digest cache: {e:#}");
                }
                Ok::<_, Error>(files)
            })
            .await;
            let sysroot = key.sysroot.display();
            let state = match hashed {
                Ok(Ok(files)) => {
                    info!("REAPI: hashed the toolchain in {sysroot}");
                    State::Hashed(Arc::new(files))
                }
                Ok(Err(e)) => match e.downcast_ref::<UnusableToolchain>() {
                    Some(UnusableToolchain(reason)) => {
                        warn!(
                            "REAPI: compiles with the toolchain in {sysroot} run locally until it changes: {e:#}"
                        );
                        State::Unusable(*reason)
                    }
                    None => {
                        warn!("REAPI: failed to hash the toolchain in {sysroot}: {e:#}");
                        this.hashing_failed()
                    }
                },
                Err(e) => {
                    if e.is_panic() {
                        warn!("REAPI: hashing the toolchain in {sysroot} panicked: {e}");
                    }
                    this.hashing_failed()
                }
            };
            this.states.lock().await.insert(key, state);
            this.hash_attempts_ended.send_modify(|count| *count += 1);
        });
    }

    fn hashing_failed(&self) -> State {
        State::HashingFailed {
            retry_at: Instant::now() + self.retry_after,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::reapi::remote_compile::test::fake_sysroot;
    use std::path::Path;

    const TARGET_TRIPLE: &str = "x86_64-unknown-linux-gnu";

    fn toolchains(dir: &Path, retry_after: Duration) -> Arc<Toolchains> {
        let digest_cache = DigestCache::load(dir.join("digests.json")).caching_new_files();
        let toolchains = Toolchains::new(digest_cache);
        Arc::new(Toolchains {
            retry_after,
            ..Arc::into_inner(toolchains).unwrap()
        })
    }

    fn key(sysroot: &Path) -> ToolchainKey {
        ToolchainKey::new(
            "abc123".to_owned(),
            sysroot.to_owned(),
            TARGET_TRIPLE.to_owned(),
        )
    }

    fn in_secs(secs: u64) -> Instant {
        Instant::now() + Duration::from_secs(secs)
    }

    #[tokio::test]
    async fn test_a_toolchain_is_hashed_once() {
        let dir = tempfile::tempdir().unwrap();
        let toolchains = toolchains(dir.path(), RETRY_AFTER);
        let key = key(&fake_sysroot(dir.path()));
        assert_eq!(
            Some(CompileLocallyReason::HashingToolchain),
            toolchains.files(&key, Instant::now()).await.err()
        );
        let compiles: Vec<_> = (0..4)
            .map(|_| {
                let (toolchains, key) = (toolchains.clone(), key.clone());
                tokio::spawn(async move { toolchains.files(&key, in_secs(10)).await.unwrap() })
            })
            .collect();
        let first = toolchains.files(&key, in_secs(10)).await.unwrap();
        for compile in compiles {
            assert!(Arc::ptr_eq(&first, &compile.await.unwrap()));
        }
    }

    #[tokio::test]
    async fn test_hashed_digests_are_saved() {
        let dir = tempfile::tempdir().unwrap();
        let sysroot = fake_sysroot(dir.path());
        let rustc = sysroot.join("bin/rustc");
        let std = sysroot.join(format!("lib/rustlib/{TARGET_TRIPLE}/lib/libstd-x.rlib"));
        let toolchains = toolchains(dir.path(), RETRY_AFTER);
        toolchains.files(&key(&sysroot), in_secs(10)).await.unwrap();
        let saved = std::fs::read_to_string(dir.path().join("digests.json")).unwrap();
        for file in [&rustc, &std] {
            assert!(
                saved.contains(file.to_str().unwrap()),
                "{file:?} not in {saved}"
            );
        }
    }

    #[tokio::test]
    async fn test_a_toolchain_without_std_is_unusable_until_it_changes() {
        let dir = tempfile::tempdir().unwrap();
        let sysroot = fake_sysroot(dir.path());
        let std_dir = sysroot.join(format!("lib/rustlib/{TARGET_TRIPLE}"));
        let moved = dir.path().join("std");
        std::fs::rename(&std_dir, &moved).unwrap();
        let toolchains = toolchains(dir.path(), Duration::ZERO);
        let without_std = key(&sysroot);
        assert_eq!(
            Some(CompileLocallyReason::MissingRustcOrStd),
            toolchains.files(&without_std, in_secs(10)).await.err()
        );

        std::fs::rename(&moved, &std_dir).unwrap();
        assert_eq!(
            Some(CompileLocallyReason::MissingRustcOrStd),
            toolchains.files(&without_std, in_secs(10)).await.err(),
            "hashed again under the same key"
        );
        let with_std = key(&sysroot);
        assert_ne!(without_std, with_std);
        assert!(toolchains.files(&with_std, in_secs(10)).await.is_ok());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_a_failed_hashing_is_retried_after_a_while() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let sysroot = fake_sysroot(dir.path());
        let rustc = sysroot.join("bin/rustc");
        let set_mode = |mode| {
            std::fs::set_permissions(&rustc, std::fs::Permissions::from_mode(mode)).unwrap();
        };
        set_mode(0o000);
        // Root reads the file anyway.
        if std::fs::read(&rustc).is_ok() {
            return;
        }
        let key = key(&sysroot);

        let waiting = toolchains(dir.path(), RETRY_AFTER);
        assert_eq!(
            Some(CompileLocallyReason::HashingToolchainFailed),
            waiting.files(&key, in_secs(10)).await.err()
        );
        set_mode(0o755);
        assert_eq!(
            Some(CompileLocallyReason::HashingToolchainFailed),
            waiting.files(&key, in_secs(10)).await.err(),
            "retried before the backoff ended"
        );

        set_mode(0o000);
        let retrying = toolchains(dir.path(), Duration::ZERO);
        assert_eq!(
            Some(CompileLocallyReason::HashingToolchainFailed),
            retrying.files(&key, in_secs(10)).await.err()
        );
        set_mode(0o755);
        assert!(retrying.files(&key, in_secs(10)).await.is_ok());
    }
}
