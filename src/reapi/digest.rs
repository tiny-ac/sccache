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

//! SHA-256 digests, which REAPI uses to identify content, and a cache of file digests.

use super::proto::build::bazel::remote::execution::v2 as remexec;
use crate::errors::*;
use crate::util::{MetadataCtimeExt, Timestamp};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

/// Files changed more recently than this aren't cached, since on a filesystem with coarse
/// timestamps a same-size edit can leave the stamp unchanged.
const MIN_AGE_TO_CACHE: Duration = Duration::from_secs(2);

const MAX_ENTRIES: usize = 100_000;

pub(crate) fn of_bytes(bytes: &[u8]) -> remexec::Digest {
    remexec::Digest {
        hash: format!("{:x}", Sha256::digest(bytes)),
        size_bytes: bytes.len() as i64,
    }
}

/// Fails unless `digest` is a SHA-256 digest. Digests from the REAPI server go into
/// ByteStream resource names, and a hash that isn't hex isn't echoed into the error.
pub(crate) fn check(digest: &remexec::Digest) -> Result<()> {
    let is_lower_hex = |c: char| c.is_ascii_digit() || ('a'..='f').contains(&c);
    if digest.hash.len() != 64 || !digest.hash.chars().all(is_lower_hex) {
        bail!("invalid REAPI digest: the hash isn't 64 lowercase hex digits");
    }
    if digest.size_bytes < 0 || (digest.size_bytes == 0 && *digest != of_bytes(b"")) {
        bail!("invalid REAPI digest {}/{}", digest.hash, digest.size_bytes);
    }
    Ok(())
}

pub(crate) fn of_file(path: &Path) -> Result<remexec::Digest> {
    let mut file =
        std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut hasher = Sha256::new();
    let size = std::io::copy(&mut file, &mut hasher)
        .with_context(|| format!("reading {}", path.display()))?;
    Ok(remexec::Digest {
        hash: format!("{:x}", hasher.finalize()),
        size_bytes: size as i64,
    })
}

/// The metadata that tells whether a file changed since it was hashed.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FileStamp {
    size: u64,
    modified: Timestamp,
    /// `None` on a Windows filesystem that doesn't record creation times.
    ctime_or_creation: Option<Timestamp>,
    #[cfg(unix)]
    inode: u64,
}

impl FileStamp {
    fn of(path: &Path) -> Result<Self> {
        // Follows symlinks, as `of_file` does.
        let metadata = std::fs::metadata(path)
            .with_context(|| format!("reading the metadata of {}", path.display()))?;
        let modified = metadata
            .modified()
            .with_context(|| format!("reading the modification time of {}", path.display()))?;
        Ok(FileStamp {
            size: metadata.len(),
            modified: modified.into(),
            ctime_or_creation: metadata.ctime_or_creation().ok(),
            #[cfg(unix)]
            inode: std::os::unix::fs::MetadataExt::ino(&metadata),
        })
    }

    fn last_change(&self) -> Timestamp {
        self.ctime_or_creation
            .map_or(self.modified, |ctime| ctime.max(self.modified))
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct DigestCacheEntry {
    stamp: FileStamp,
    hash: String,
    #[serde(skip)]
    pinned: bool,
}

/// Caches file digests by absolute path, and hashes a file again when it changes. Pinned
/// entries are the ones [`DigestCache::save`] writes for [`DigestCache::load`]; a full cache
/// drops every entry that isn't pinned.
///
/// Its methods block, so call them from a blocking thread.
pub(crate) struct DigestCache {
    entries: Mutex<HashMap<PathBuf, DigestCacheEntry>>,
    save_file: PathBuf,
    min_age_to_cache: Duration,
    max_entries: usize,
}

impl DigestCache {
    /// Loads the entries saved in `save_file` as pinned, except those of deleted files. Starts
    /// empty if `save_file` is missing or, with a warning, unreadable.
    pub(crate) fn load(save_file: PathBuf) -> Self {
        let parsed = match std::fs::read(&save_file) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(Error::from),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
            Err(e) => Err(e.into()),
        };
        let saved: HashMap<PathBuf, DigestCacheEntry> = parsed.unwrap_or_else(|e| {
            warn!(
                "REAPI: ignoring the digest cache's unreadable save file {}: {e}",
                save_file.display()
            );
            HashMap::new()
        });
        let entries = saved
            .into_iter()
            .filter(|(path, _)| path.exists())
            .map(|(path, entry)| {
                (
                    path,
                    DigestCacheEntry {
                        pinned: true,
                        ..entry
                    },
                )
            })
            .collect();
        DigestCache {
            entries: Mutex::new(entries),
            save_file,
            min_age_to_cache: MIN_AGE_TO_CACHE,
            max_entries: MAX_ENTRIES,
        }
    }

    pub(crate) fn digest(&self, path: &Path) -> Result<remexec::Digest> {
        self.get_or_hash(path, false)
    }

    /// Like [`DigestCache::digest`], and pins the entry. Pinned entries are never dropped, so
    /// pin only a few files that are costly to hash.
    pub(crate) fn digest_and_pin(&self, path: &Path) -> Result<remexec::Digest> {
        self.get_or_hash(path, true)
    }

    fn get_or_hash(&self, path: &Path, pin: bool) -> Result<remexec::Digest> {
        // Read the clock first, so a file edited during hashing without a stamp change is
        // too recent to cache.
        let latest_cacheable_change = Timestamp::from(SystemTime::now() - self.min_age_to_cache);
        debug_assert!(
            path.is_absolute(),
            "the digest cache needs absolute paths: {path:?}"
        );
        let stamp = FileStamp::of(path)?;
        if let Some(entry) = self.entries.lock().unwrap().get_mut(path)
            && entry.stamp == stamp
        {
            entry.pinned |= pin;
            return Ok(remexec::Digest {
                hash: entry.hash.clone(),
                size_bytes: stamp.size as i64,
            });
        }
        let digest = of_file(path)?;
        // A file that changes, or can no longer be stat'ed, during hashing isn't cached.
        if stamp.last_change() <= latest_cacheable_change
            && FileStamp::of(path).is_ok_and(|restamp| restamp == stamp)
        {
            let mut entries = self.entries.lock().unwrap();
            let is_cached = entries.contains_key(path);
            if entries.len() >= self.max_entries && !is_cached {
                entries.retain(|_, entry| entry.pinned);
            }
            let pinned = pin || entries.get(path).is_some_and(|entry| entry.pinned);
            // Replacing an entry doesn't grow the cache.
            if pinned || is_cached || entries.len() < self.max_entries {
                entries.insert(
                    path.to_owned(),
                    DigestCacheEntry {
                        stamp,
                        hash: digest.hash.clone(),
                        pinned,
                    },
                );
            }
        }
        Ok(digest)
    }

    /// Caches files however recently they changed.
    #[cfg(test)]
    pub(super) fn caching_new_files(mut self) -> Self {
        self.min_age_to_cache = Duration::ZERO;
        self
    }

    pub(crate) fn save(&self) -> Result<()> {
        let to_save: HashMap<PathBuf, DigestCacheEntry> = self
            .entries
            .lock()
            .unwrap()
            .iter()
            // JSON keys must be UTF-8.
            .filter(|(path, entry)| entry.pinned && path.to_str().is_some())
            .map(|(path, entry)| (path.clone(), entry.clone()))
            .collect();
        let dir = self
            .save_file
            .parent()
            .context("the digest cache's save file has no directory")?;
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        let mut tmp_file = tempfile::NamedTempFile::new_in(dir)
            .with_context(|| format!("creating a temporary file in {}", dir.display()))?;
        tmp_file
            .write_all(&serde_json::to_vec(&to_save)?)
            .with_context(|| format!("writing {}", tmp_file.path().display()))?;
        tmp_file.persist(&self.save_file).with_context(|| {
            format!("moving the temporary file to {}", self.save_file.display())
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use test_case::test_case;

    fn load_without_min_age(save_file: PathBuf) -> DigestCache {
        let mut cache = DigestCache::load(save_file);
        cache.min_age_to_cache = Duration::ZERO;
        cache
    }

    fn set_mtime(path: &Path, unix_time: i64) {
        let mtime = filetime::FileTime::from_unix_time(unix_time, 0);
        filetime::set_file_mtime(path, mtime).unwrap();
    }

    fn read_saved_entries(save_file: &Path) -> HashMap<PathBuf, DigestCacheEntry> {
        serde_json::from_slice(&std::fs::read(save_file).unwrap()).unwrap()
    }

    #[test]
    fn test_check_accepts_sha256_digests() {
        check(&of_bytes(b"")).unwrap();
        check(&of_bytes(b"x")).unwrap();
    }

    #[test_case("../../etc/passwd", 5 ; "not a sha256 hash")]
    #[test_case(&of_bytes(b"x").hash.to_uppercase(), 1 ; "uppercase hash")]
    #[test_case(&of_bytes(b"x").hash, -1 ; "negative size")]
    #[test_case(&of_bytes(b"x").hash, 0 ; "empty size with a non-empty hash")]
    fn test_check_refuses_invalid_digests(hash: &str, size_bytes: i64) {
        let invalid = remexec::Digest {
            hash: hash.to_owned(),
            size_bytes,
        };
        let e = check(&invalid).unwrap_err();
        assert!(e.to_string().contains("invalid REAPI digest"), "{e}");
        assert!(!e.to_string().contains("passwd"), "{e}");
    }

    #[test]
    fn test_digest_is_sha256() {
        assert_eq!(
            remexec::Digest {
                hash: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".into(),
                size_bytes: 5,
            },
            of_bytes(b"hello")
        );
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f");
        std::fs::write(&file, b"hello").unwrap();
        assert_eq!(of_bytes(b"hello"), of_file(&file).unwrap());
    }

    #[test]
    fn test_saved_digests_are_reused_until_the_file_changes() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f");
        std::fs::write(&file, b"hello").unwrap();
        let save_file = dir.path().join("digests.json");

        let cache = load_without_min_age(save_file.clone());
        assert_eq!(of_bytes(b"hello"), cache.digest_and_pin(&file).unwrap());
        cache.save().unwrap();

        // A doctored hash shows that a new cache uses the saved entry without hashing.
        let mut entries = read_saved_entries(&save_file);
        entries.get_mut(&file).unwrap().hash = "saved".into();
        std::fs::write(&save_file, serde_json::to_vec(&entries).unwrap()).unwrap();
        let cache = load_without_min_age(save_file);
        assert_eq!("saved", cache.digest_and_pin(&file).unwrap().hash);

        // A same-size edit, with an mtime that differs from the saved stamp's.
        std::fs::write(&file, b"howdy").unwrap();
        set_mtime(&file, 1_700_000_000);
        assert_eq!(of_bytes(b"howdy"), cache.digest_and_pin(&file).unwrap());
    }

    // Windows stamps have no inode, only a creation time.
    #[cfg(unix)]
    #[test]
    fn test_a_file_replaced_by_rename_is_hashed_again() {
        let dir = tempfile::tempdir().unwrap();
        let (file, replacement) = (dir.path().join("f"), dir.path().join("g"));
        std::fs::write(&file, b"hello").unwrap();
        std::fs::write(&replacement, b"howdy").unwrap();
        set_mtime(&file, 1_700_000_000);
        set_mtime(&replacement, 1_700_000_000);
        let cache = load_without_min_age(dir.path().join("digests.json"));
        cache.digest(&file).unwrap();
        std::fs::rename(&replacement, &file).unwrap();
        assert_eq!(of_bytes(b"howdy"), cache.digest(&file).unwrap());
    }

    #[test]
    fn test_recently_changed_files_are_not_cached() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f");
        std::fs::write(&file, b"hello").unwrap();
        let mut cache = DigestCache::load(dir.path().join("digests.json"));
        cache.min_age_to_cache = Duration::from_secs(3600);
        cache.digest(&file).unwrap();
        assert!(cache.entries.lock().unwrap().is_empty());
        cache.min_age_to_cache = Duration::ZERO;
        cache.digest(&file).unwrap();
        assert_eq!(1, cache.entries.lock().unwrap().len());
    }

    #[test]
    fn test_files_written_with_an_old_mtime_are_not_cached_right_away() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f");
        std::fs::write(&file, b"hello").unwrap();
        // As when extracting an archive that keeps its files' mtimes.
        set_mtime(&file, 1_700_000_000);
        let mut cache = DigestCache::load(dir.path().join("digests.json"));
        cache.min_age_to_cache = Duration::from_secs(3600);
        cache.digest(&file).unwrap();
        assert!(cache.entries.lock().unwrap().is_empty());
    }

    #[test]
    fn test_only_pinned_entries_are_saved() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (dir.path().join("a"), dir.path().join("b"));
        std::fs::write(&a, b"a").unwrap();
        std::fs::write(&b, b"b").unwrap();
        let save_file = dir.path().join("digests.json");
        let cache = load_without_min_age(save_file.clone());
        cache.digest_and_pin(&a).unwrap();
        cache.digest(&b).unwrap();
        cache.save().unwrap();
        let entries = read_saved_entries(&save_file);
        assert!(entries.contains_key(&a));
        assert!(!entries.contains_key(&b));
    }

    #[test]
    fn test_a_pinned_entry_stays_pinned_after_its_file_changes() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f");
        std::fs::write(&file, b"hello").unwrap();
        let save_file = dir.path().join("digests.json");
        let cache = load_without_min_age(save_file.clone());
        cache.digest(&file).unwrap();
        // A cache hit pins the entry.
        cache.digest_and_pin(&file).unwrap();
        std::fs::write(&file, b"howdy").unwrap();
        set_mtime(&file, 1_700_000_000);
        cache.digest(&file).unwrap();
        cache.save().unwrap();
        assert_eq!(
            of_bytes(b"howdy").hash,
            read_saved_entries(&save_file)[&file].hash
        );
    }

    #[test]
    fn test_a_full_cache_drops_entries_that_are_not_pinned() {
        let dir = tempfile::tempdir().unwrap();
        let [pinned_file, a, b] = ["pinned", "a", "b"].map(|name| {
            let path = dir.path().join(name);
            std::fs::write(&path, name).unwrap();
            path
        });
        let mut cache = load_without_min_age(dir.path().join("digests.json"));
        cache.max_entries = 2;
        cache.digest_and_pin(&pinned_file).unwrap();
        cache.digest(&a).unwrap();
        cache.digest(&b).unwrap();
        let entries = cache.entries.lock().unwrap();
        let mut paths: Vec<_> = entries.keys().collect();
        paths.sort();
        assert_eq!(vec![&b, &pinned_file], paths);
    }

    #[test]
    fn test_a_cache_full_of_pinned_entries_caches_no_others() {
        let dir = tempfile::tempdir().unwrap();
        let [pinned_file, a] = ["pinned", "a"].map(|name| {
            let path = dir.path().join(name);
            std::fs::write(&path, name).unwrap();
            path
        });
        let mut cache = load_without_min_age(dir.path().join("digests.json"));
        cache.max_entries = 1;
        cache.digest_and_pin(&pinned_file).unwrap();
        assert_eq!(of_bytes(b"a"), cache.digest(&a).unwrap());
        let entries = cache.entries.lock().unwrap();
        assert_eq!(vec![&pinned_file], entries.keys().collect::<Vec<_>>());
    }

    #[test]
    fn test_a_full_cache_replaces_the_entry_of_a_changed_file() {
        let dir = tempfile::tempdir().unwrap();
        let [a, b] = ["a", "b"].map(|name| {
            let path = dir.path().join(name);
            std::fs::write(&path, name).unwrap();
            path
        });
        let mut cache = load_without_min_age(dir.path().join("digests.json"));
        cache.max_entries = 2;
        cache.digest(&a).unwrap();
        cache.digest(&b).unwrap();
        std::fs::write(&a, b"A").unwrap();
        set_mtime(&a, 1_700_000_000);
        cache.digest(&a).unwrap();
        let entries = cache.entries.lock().unwrap();
        assert_eq!(of_bytes(b"A").hash, entries[&a].hash);
        assert!(entries.contains_key(&b));
    }

    #[test]
    fn test_an_unreadable_save_file_starts_an_empty_cache() {
        let dir = tempfile::tempdir().unwrap();
        let save_file = dir.path().join("digests.json");
        std::fs::write(&save_file, b"not json").unwrap();
        assert!(
            DigestCache::load(save_file)
                .entries
                .lock()
                .unwrap()
                .is_empty()
        );
    }

    // macOS refuses non-UTF-8 file names.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_save_skips_non_utf8_paths() {
        use std::os::unix::ffi::OsStrExt;
        let dir = tempfile::tempdir().unwrap();
        let utf8 = dir.path().join("utf8");
        let non_utf8 = dir.path().join(std::ffi::OsStr::from_bytes(b"\xff"));
        std::fs::write(&utf8, b"1").unwrap();
        std::fs::write(&non_utf8, b"2").unwrap();
        let save_file = dir.path().join("digests.json");
        let cache = load_without_min_age(save_file.clone());
        cache.digest_and_pin(&utf8).unwrap();
        cache.digest_and_pin(&non_utf8).unwrap();
        cache.save().unwrap();
        assert!(read_saved_entries(&save_file).contains_key(&utf8));
    }

    #[test]
    fn test_entries_of_deleted_files_are_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let (kept, deleted) = (dir.path().join("kept"), dir.path().join("deleted"));
        std::fs::write(&kept, b"k").unwrap();
        std::fs::write(&deleted, b"d").unwrap();
        let save_file = dir.path().join("digests.json");
        let cache = load_without_min_age(save_file.clone());
        cache.digest_and_pin(&kept).unwrap();
        cache.digest_and_pin(&deleted).unwrap();
        cache.save().unwrap();
        std::fs::remove_file(&deleted).unwrap();

        DigestCache::load(save_file.clone()).save().unwrap();
        let entries = read_saved_entries(&save_file);
        assert!(entries.contains_key(&kept));
        assert!(!entries.contains_key(&deleted));
    }
}
