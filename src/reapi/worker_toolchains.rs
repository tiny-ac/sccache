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

//! Linux toolchains for clients that don't run Linux, taken from the `[[dist.toolchains]]`
//! archives sccache-dist uses: a tar.gz of a Linux toolchain whose std includes the client's
//! target. sccache unpacks the toolchain's sysroot from each version of an archive once, under
//! the dist cache dir, in a separate task that no compile waits for past its deadline.

use super::mapping::{CompileLocallyReason, ToolchainKey};
use super::sysroot::create_private_dir;
use crate::config::DistToolchainConfig;
use crate::errors::*;
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::io::BufReader;
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::watch;

const RETRY_AFTER: Duration = Duration::from_secs(60);

pub(super) struct WorkerToolchains {
    /// The archives, by the local rustc they stand in for.
    archives: HashMap<PathBuf, ToolchainArchive>,
    dir: PathBuf,
    /// By archive path and stamp, so that a changed archive is unpacked again.
    unpacks: Mutex<HashMap<(PathBuf, Option<Stamp>), watch::Receiver<UnpackState>>>,
}

/// An archive file's size and modification time.
type Stamp = (u64, SystemTime);

#[derive(Clone)]
struct ToolchainArchive {
    path: PathBuf,
    /// As `archive_compiler_executable` gives it.
    rustc_path: String,
}

#[derive(Clone)]
enum UnpackState {
    Running,
    /// The sysroot of the archive's rustc.
    Done(PathBuf),
    Failed {
        at: Instant,
    },
}

impl WorkerToolchains {
    /// The `path_override` archives of `toolchains`, unpacked under `dir` when first needed.
    pub(super) fn new(toolchains: &[DistToolchainConfig], dir: PathBuf) -> Self {
        let mut archives = HashMap::new();
        for toolchain in toolchains {
            let DistToolchainConfig::PathOverride {
                compiler_executable,
                archive,
                archive_compiler_executable,
            } = toolchain
            else {
                continue;
            };
            if archives.contains_key(compiler_executable) {
                warn!(
                    "REAPI: [[dist.toolchains]] has more than one archive for {}; using the first",
                    compiler_executable.display()
                );
                continue;
            }
            archives.insert(
                compiler_executable.clone(),
                ToolchainArchive {
                    path: archive.clone(),
                    rustc_path: archive_compiler_executable.clone(),
                },
            );
        }
        WorkerToolchains {
            archives,
            dir,
            unpacks: Default::default(),
        }
    }

    /// The toolchain workers run in place of `local`, from the archive configured for
    /// `local`'s rustc, or `None` without one. An archive that fails to unpack is tried again
    /// after [`RETRY_AFTER`], or once it changes.
    pub(super) async fn toolchain(
        &self,
        local: &ToolchainKey,
        deadline: Instant,
    ) -> std::result::Result<Option<ToolchainKey>, CompileLocallyReason> {
        // The archives hold rustc toolchains.
        let ToolchainKey::Rustc(local) = local else {
            return Ok(None);
        };
        let rustc = local.sysroot.join("bin/rustc");
        let Some(archive) = self.archives.get(&rustc) else {
            debug!(
                "REAPI: no [[dist.toolchains]] archive for {}",
                rustc.display()
            );
            return Ok(None);
        };
        let path = archive.path.clone();
        let stamp = tokio::task::spawn_blocking(move || {
            let metadata = std::fs::metadata(path).ok()?;
            Some((metadata.len(), metadata.modified().ok()?))
        })
        .await
        .map_err(|_| CompileLocallyReason::UnusableWorkerToolchain)?;
        let mut unpack = self.unpack(archive, stamp);
        let unpacked = unpack.wait_for(|state| !matches!(state, UnpackState::Running));
        // The `watch::Ref` isn't `Send`, so it's dropped before the next await.
        let sysroot = match tokio::time::timeout_at(deadline.into(), unpacked).await {
            Err(_) => return Err(CompileLocallyReason::UnpackingWorkerToolchain),
            Ok(Ok(state)) => match &*state {
                UnpackState::Done(sysroot) => sysroot.clone(),
                _ => return Err(CompileLocallyReason::UnusableWorkerToolchain),
            },
            Ok(Err(_)) => return Err(CompileLocallyReason::UnusableWorkerToolchain),
        };
        let local = local.clone();
        tokio::task::spawn_blocking(move || {
            if !same_std(&local.sysroot, &sysroot, &local.target_triple) {
                return Err(CompileLocallyReason::WorkerStdDiffers);
            }
            // The key reads the sysroot's stamps.
            Ok(Some(ToolchainKey::rustc(
                local.commit_hash,
                sysroot,
                local.target_triple,
            )))
        })
        .await
        .unwrap_or(Err(CompileLocallyReason::UnusableWorkerToolchain))
    }

    /// The unpack of `archive` at `stamp`, started unless it runs, is done, or failed in the
    /// last [`RETRY_AFTER`].
    fn unpack(
        &self,
        archive: &ToolchainArchive,
        stamp: Option<Stamp>,
    ) -> watch::Receiver<UnpackState> {
        let mut unpacks = self.unpacks.lock().unwrap();
        let key = (archive.path.clone(), stamp);
        let retry = match unpacks.get(&key) {
            None => true,
            Some(unpack) => {
                matches!(*unpack.borrow(), UnpackState::Failed { at } if at.elapsed() >= RETRY_AFTER)
            }
        };
        if retry {
            unpacks.retain(|(path, _), _| *path != archive.path);
            unpacks.insert(key.clone(), self.spawn_unpack(archive.clone()));
        }
        unpacks[&key].clone()
    }

    fn spawn_unpack(&self, archive: ToolchainArchive) -> watch::Receiver<UnpackState> {
        let (sender, receiver) = watch::channel(UnpackState::Running);
        let dir = self.dir.clone();
        tokio::spawn(async move {
            let path = archive.path.clone();
            let sysroot = tokio::task::spawn_blocking(move || unpack(&archive, &dir))
                .await
                .map_err(Error::from)
                .flatten();
            sender.send_replace(match sysroot {
                Ok(sysroot) => UnpackState::Done(sysroot),
                Err(e) => {
                    warn!(
                        "REAPI: {e:#}; compiling locally what needs {} for {} seconds",
                        path.display(),
                        RETRY_AFTER.as_secs()
                    );
                    UnpackState::Failed { at: Instant::now() }
                }
            });
        });
        receiver
    }
}

/// Unpacks the sysroot of `archive`'s rustc under `dir`, once for each size and modification
/// time of the archive file, removes the earlier versions, and returns the sysroot.
fn unpack(archive: &ToolchainArchive, dir: &Path) -> Result<PathBuf> {
    let rustc_path = Path::new(&archive.rustc_path);
    let rustc_path = rustc_path.strip_prefix("/").unwrap_or(rustc_path);
    let sysroot_in_archive = rustc_path
        .parent()
        .and_then(Path::parent)
        .filter(|_| {
            rustc_path.ends_with("bin/rustc")
                && rustc_path
                    .components()
                    .all(|component| matches!(component, Component::Normal(_)))
        })
        .with_context(|| {
            format!(
                "archive_compiler_executable {:?} must be a <sysroot>/bin/rustc path in the \
                 archive, without . or ..",
                archive.rustc_path
            )
        })?;
    let metadata = std::fs::metadata(&archive.path)
        .with_context(|| format!("failed to read {}", archive.path.display()))?;
    let modified = metadata
        .modified()
        .with_context(|| format!("failed to read the mtime of {}", archive.path.display()))?;
    let archive_dir = dir.join(
        blake3::hash(archive.path.as_os_str().as_encoded_bytes())
            .to_hex()
            .as_str(),
    );
    let version_dir = archive_dir.join(format!(
        "{}-{}",
        metadata.len(),
        modified
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    if !version_dir.is_dir() {
        create_private_dir(&archive_dir)
            .with_context(|| format!("failed to create {}", archive_dir.display()))?;
        let unpacking = tempfile::Builder::new()
            .prefix(".unpacking")
            .tempdir_in(&archive_dir)
            .with_context(|| {
                format!("failed to create a directory in {}", archive_dir.display())
            })?;
        unpack_sysroot(&archive.path, sysroot_in_archive, unpacking.path())?;
        check_symlinks(&unpacking.path().join(sysroot_in_archive))
            .with_context(|| format!("unusable toolchain in {}", archive.path.display()))?;
        // Another sccache server may have unpacked the same archive meanwhile.
        if let Err(e) = std::fs::rename(unpacking.path(), &version_dir)
            && !version_dir.is_dir()
        {
            return Err(e).with_context(|| format!("failed to create {}", version_dir.display()));
        }
        remove_other_versions(&archive_dir, &version_dir);
    }
    let sysroot = version_dir.join(sysroot_in_archive);
    if !sysroot.join("bin/rustc").is_file() {
        bail!(
            "no rustc at {} in {}",
            archive.rustc_path,
            archive.path.display()
        );
    }
    Ok(sysroot)
}

/// Unpacks the entries of `archive` under `sysroot` into `destination`.
fn unpack_sysroot(archive: &Path, sysroot: &Path, destination: &Path) -> Result<()> {
    let file = std::fs::File::open(archive)
        .with_context(|| format!("failed to open {}", archive.display()))?;
    let mut tar = tar::Archive::new(flate2::read::GzDecoder::new(BufReader::new(file)));
    let unpack_failed = || format!("failed to unpack {}", archive.display());
    for entry in tar.entries().with_context(unpack_failed)? {
        let mut entry = entry.with_context(unpack_failed)?;
        let path: PathBuf = (entry.path().with_context(unpack_failed)?.components())
            .filter(|component| matches!(component, Component::Normal(_)))
            .collect();
        if path.starts_with(sysroot) {
            entry.unpack_in(destination).with_context(unpack_failed)?;
        }
    }
    Ok(())
}

/// Fails on a symlink under `sysroot` that resolves out of it, or nowhere, since the
/// toolchain's files are read through their symlinks and uploaded. The symlinks are resolved
/// on disk, as a `..` after a symlink climbs from where that symlink points.
fn check_symlinks(sysroot: &Path) -> Result<()> {
    let sysroot = sysroot
        .canonicalize()
        .with_context(|| format!("no {}", sysroot.display()))?;
    for entry in walkdir::WalkDir::new(&sysroot) {
        let entry = entry?;
        if entry.path_is_symlink()
            && !(entry.path().canonicalize()).is_ok_and(|target| target.starts_with(&sysroot))
        {
            let link = entry.path().strip_prefix(&sysroot).unwrap_or(entry.path());
            bail!("{} links out of the toolchain", link.display());
        }
    }
    Ok(())
}

/// Removes the unpacked versions in `archive_dir` other than `current`. Directories that are
/// still being unpacked, whose names start with a dot, stay.
fn remove_other_versions(archive_dir: &Path, current: &Path) {
    let Ok(entries) = std::fs::read_dir(archive_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path != current
            && !entry.file_name().to_string_lossy().starts_with('.')
            && let Err(e) = std::fs::remove_dir_all(&path)
        {
            debug!("REAPI: failed to remove {}: {e}", path.display());
        }
    }
}

/// Whether the `.rlib` and `.rmeta` files of `worker`'s std for `target` have the names and
/// sizes of `local`'s. rust-std for a target is the same on every host, so they differ for
/// another rustc version, or a worker toolchain without the target. Other files there can
/// differ: the host's toolchain keeps libraries such as LLVM's next to its std.
fn same_std(local: &Path, worker: &Path, target: &str) -> bool {
    let files = |sysroot: &Path| -> Option<BTreeMap<OsString, u64>> {
        let lib_dir = sysroot.join("lib/rustlib").join(target).join("lib");
        let mut files = BTreeMap::new();
        for entry in std::fs::read_dir(lib_dir).ok()? {
            let path = entry.ok()?.path();
            if path
                .extension()
                .is_some_and(|extension| extension == "rlib" || extension == "rmeta")
            {
                let size = std::fs::metadata(&path).ok()?.len();
                files.insert(path.file_name()?.to_owned(), size);
            }
        }
        Some(files)
    };
    files(local) == files(worker)
}

#[cfg(test)]
mod test {
    use super::*;
    use test_case::test_case;

    const TARGET: &str = "aarch64-apple-darwin";

    fn tar_file(builder: &mut tar::Builder<impl std::io::Write>, name: &str, contents: &[u8]) {
        let mut header = tar::Header::new_gnu();
        header.set_size(contents.len() as u64);
        header.set_mode(0o755);
        header.set_cksum();
        builder.append_data(&mut header, name, contents).unwrap();
    }

    /// A tar.gz in `dir` of a Linux root with a toolchain at `opt/toolchain`, its rustc and
    /// `TARGET`'s std, and `extra` added to `builder` before it's written.
    fn archive_with(
        dir: &Path,
        extra: impl FnOnce(&mut tar::Builder<flate2::write::GzEncoder<std::fs::File>>),
    ) -> PathBuf {
        let path = dir.join("toolchain.tar.gz");
        let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
            std::fs::File::create(&path).unwrap(),
            flate2::Compression::fast(),
        ));
        tar_file(&mut builder, "etc/passwd", b"root");
        tar_file(&mut builder, "opt/toolchain/bin/rustc", b"rustc");
        tar_file(
            &mut builder,
            &format!("opt/toolchain/lib/rustlib/{TARGET}/lib/libstd.rlib"),
            b"std",
        );
        extra(&mut builder);
        builder.into_inner().unwrap().finish().unwrap();
        path
    }

    fn archive(dir: &Path) -> PathBuf {
        archive_with(dir, |_| {})
    }

    /// A local sysroot in `dir` with a rustc and `TARGET`'s std of `contents`.
    fn local_sysroot(dir: &Path, contents: &[u8]) -> PathBuf {
        let sysroot = dir.join("local");
        std::fs::create_dir_all(sysroot.join("bin")).unwrap();
        std::fs::write(sysroot.join("bin/rustc"), "rustc").unwrap();
        let lib_dir = sysroot.join("lib/rustlib").join(TARGET).join("lib");
        std::fs::create_dir_all(&lib_dir).unwrap();
        std::fs::write(lib_dir.join("libstd.rlib"), contents).unwrap();
        sysroot
    }

    fn worker_toolchains(dir: &Path, archive: PathBuf, rustc_path: &str) -> WorkerToolchains {
        WorkerToolchains::new(
            &[DistToolchainConfig::PathOverride {
                compiler_executable: dir.join("local/bin/rustc"),
                archive,
                archive_compiler_executable: rustc_path.to_owned(),
            }],
            dir.join("worker-toolchains"),
        )
    }

    fn local_toolchain(sysroot: &Path) -> ToolchainKey {
        ToolchainKey::rustc("abc".to_owned(), sysroot.to_owned(), TARGET.to_owned())
    }

    fn in_secs(secs: u64) -> Instant {
        Instant::now() + Duration::from_secs(secs)
    }

    /// The directories that hold unpacked versions in `dir`.
    fn versions(dir: &Path) -> Vec<PathBuf> {
        std::fs::read_dir(dir.join("worker-toolchains"))
            .unwrap()
            .flat_map(|archive_dir| std::fs::read_dir(archive_dir.unwrap().path()).unwrap())
            .map(|version| version.unwrap().path())
            .collect()
    }

    #[tokio::test]
    async fn test_the_sysroot_of_the_archive_for_the_local_rustc_is_unpacked_once() {
        let dir = tempfile::tempdir().unwrap();
        let local = local_toolchain(&local_sysroot(dir.path(), b"std"));
        let worker_toolchains =
            worker_toolchains(dir.path(), archive(dir.path()), "/opt/toolchain/bin/rustc");
        let toolchain = worker_toolchains
            .toolchain(&local, in_secs(10))
            .await
            .unwrap()
            .unwrap();
        let rustc = toolchain.as_rustc();
        assert_eq!("abc", rustc.commit_hash);
        assert_eq!(TARGET, rustc.target_triple);
        assert!(rustc.sysroot.ends_with("opt/toolchain"));
        assert!(rustc.sysroot.join("bin/rustc").is_file());
        let version = &versions(dir.path())[0];
        assert!(
            !version.join("etc").exists(),
            "only the sysroot is unpacked"
        );
        let again = worker_toolchains
            .toolchain(&local, in_secs(10))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(toolchain, again);
        assert_eq!(1, versions(dir.path()).len());
    }

    #[tokio::test]
    async fn test_a_changed_archive_replaces_the_unpacked_version() {
        let dir = tempfile::tempdir().unwrap();
        let local = local_toolchain(&local_sysroot(dir.path(), b"std"));
        let worker_toolchains =
            worker_toolchains(dir.path(), archive(dir.path()), "/opt/toolchain/bin/rustc");
        let first = worker_toolchains
            .toolchain(&local, in_secs(10))
            .await
            .unwrap()
            .unwrap();
        archive_with(dir.path(), |builder| {
            tar_file(builder, "opt/toolchain/lib/librustc_driver.so", b"driver");
        });
        let second = worker_toolchains
            .toolchain(&local, in_secs(10))
            .await
            .unwrap()
            .unwrap();
        let (first, second) = (first.as_rustc(), second.as_rustc());
        assert_ne!(first.sysroot, second.sysroot);
        assert!(second.sysroot.join("lib/librustc_driver.so").is_file());
        assert_eq!(
            1,
            versions(dir.path()).len(),
            "the first version is removed"
        );
        assert_eq!(1, worker_toolchains.unpacks.lock().unwrap().len());
    }

    #[tokio::test]
    async fn test_an_archive_unpacked_before_a_restart_is_used_as_found() {
        let dir = tempfile::tempdir().unwrap();
        let local = local_toolchain(&local_sysroot(dir.path(), b"std"));
        let archive = archive(dir.path());
        let toolchain = worker_toolchains(dir.path(), archive.clone(), "/opt/toolchain/bin/rustc")
            .toolchain(&local, in_secs(10))
            .await
            .unwrap();
        // Contents that don't unpack, with the same size and modification time.
        let modified =
            filetime::FileTime::from_last_modification_time(&std::fs::metadata(&archive).unwrap());
        let size = std::fs::metadata(&archive).unwrap().len() as usize;
        std::fs::write(&archive, vec![b'x'; size]).unwrap();
        filetime::set_file_mtime(&archive, modified).unwrap();
        let after_restart = worker_toolchains(dir.path(), archive, "/opt/toolchain/bin/rustc")
            .toolchain(&local, in_secs(10))
            .await
            .unwrap();
        assert_eq!(toolchain, after_restart);
    }

    #[tokio::test]
    async fn test_the_first_archive_for_a_rustc_is_used() {
        let dir = tempfile::tempdir().unwrap();
        let local = local_toolchain(&local_sysroot(dir.path(), b"std"));
        let entry = |archive: PathBuf| DistToolchainConfig::PathOverride {
            compiler_executable: dir.path().join("local/bin/rustc"),
            archive,
            archive_compiler_executable: "/opt/toolchain/bin/rustc".to_owned(),
        };
        let worker_toolchains = WorkerToolchains::new(
            &[
                entry(archive(dir.path())),
                entry(dir.path().join("missing.tar.gz")),
            ],
            dir.path().join("worker-toolchains"),
        );
        assert!(
            worker_toolchains
                .toolchain(&local, in_secs(10))
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn test_another_rustc_has_no_worker_toolchain() {
        let dir = tempfile::tempdir().unwrap();
        let worker_toolchains =
            worker_toolchains(dir.path(), archive(dir.path()), "/opt/toolchain/bin/rustc");
        assert_eq!(
            Ok(None),
            worker_toolchains
                .toolchain(&local_toolchain(Path::new("/other")), in_secs(10))
                .await
        );
    }

    #[tokio::test]
    async fn test_libraries_next_to_the_local_std_dont_count() {
        let dir = tempfile::tempdir().unwrap();
        let sysroot = local_sysroot(dir.path(), b"std");
        let lib_dir = sysroot.join("lib/rustlib").join(TARGET).join("lib");
        std::fs::write(lib_dir.join("libLLVM.dylib"), "llvm").unwrap();
        let worker_toolchains =
            worker_toolchains(dir.path(), archive(dir.path()), "/opt/toolchain/bin/rustc");
        assert!(
            worker_toolchains
                .toolchain(&local_toolchain(&sysroot), in_secs(10))
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn test_a_worker_toolchain_with_another_std_compiles_locally() {
        let dir = tempfile::tempdir().unwrap();
        let local = local_toolchain(&local_sysroot(dir.path(), b"other std"));
        let worker_toolchains =
            worker_toolchains(dir.path(), archive(dir.path()), "/opt/toolchain/bin/rustc");
        assert_eq!(
            Err(CompileLocallyReason::WorkerStdDiffers),
            worker_toolchains.toolchain(&local, in_secs(10)).await
        );
    }

    #[test_case("/opt/toolchain/bin/cargo" ; "not rustc")]
    #[test_case("/opt/other/bin/rustc" ; "no rustc there")]
    #[test_case("/../../../local/bin/rustc" ; "out of the archive")]
    #[tokio::test]
    async fn test_an_unusable_archive_compiles_locally(rustc_path: &str) {
        let dir = tempfile::tempdir().unwrap();
        let local = local_toolchain(&local_sysroot(dir.path(), b"std"));
        let worker_toolchains = worker_toolchains(dir.path(), archive(dir.path()), rustc_path);
        assert_eq!(
            Err(CompileLocallyReason::UnusableWorkerToolchain),
            worker_toolchains.toolchain(&local, in_secs(10)).await
        );
    }

    /// An archive with `links`, symlinks under the toolchain as (path, target).
    fn archive_with_links(dir: &Path, links: &[(&str, &str)]) -> PathBuf {
        archive_with(dir, |builder| {
            for (path, target) in links {
                let mut header = tar::Header::new_gnu();
                header.set_entry_type(tar::EntryType::Symlink);
                header.set_size(0);
                let path = format!("opt/toolchain/{path}");
                builder.append_link(&mut header, path, target).unwrap();
            }
        })
    }

    #[test_case(&[("lib/libz.so", "/etc/passwd")] ; "absolute")]
    #[test_case(&[("lib/libz.so", "../../../etc/passwd")] ; "relative")]
    #[test_case(&[("lib/up", ".."), ("lib/x", "up/../..")] ; "parent of a symlink")]
    #[test_case(&[("lib/libz.so", "libz.so.1")] ; "dangling")]
    #[tokio::test]
    async fn test_an_archive_with_a_symlink_out_of_the_toolchain_compiles_locally(
        links: &[(&str, &str)],
    ) {
        let dir = tempfile::tempdir().unwrap();
        let local = local_toolchain(&local_sysroot(dir.path(), b"std"));
        let archive = archive_with_links(dir.path(), links);
        let worker_toolchains = worker_toolchains(dir.path(), archive, "/opt/toolchain/bin/rustc");
        assert_eq!(
            Err(CompileLocallyReason::UnusableWorkerToolchain),
            worker_toolchains.toolchain(&local, in_secs(10)).await
        );
    }

    #[tokio::test]
    async fn test_an_archive_with_symlinks_within_the_toolchain_is_used() {
        let dir = tempfile::tempdir().unwrap();
        let local = local_toolchain(&local_sysroot(dir.path(), b"std"));
        let archive = archive_with_links(
            dir.path(),
            &[("lib/up", ".."), ("lib/rustc", "up/bin/rustc")],
        );
        let worker_toolchains = worker_toolchains(dir.path(), archive, "/opt/toolchain/bin/rustc");
        assert!(
            worker_toolchains
                .toolchain(&local, in_secs(10))
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn test_a_failed_unpack_is_tried_again_after_a_while() {
        let dir = tempfile::tempdir().unwrap();
        let local = local_toolchain(&local_sysroot(dir.path(), b"std"));
        let worker_toolchains = worker_toolchains(
            dir.path(),
            dir.path().join("missing.tar.gz"),
            "/opt/toolchain/bin/rustc",
        );
        assert_eq!(
            Err(CompileLocallyReason::UnusableWorkerToolchain),
            worker_toolchains.toolchain(&local, in_secs(10)).await
        );
        let archive = &worker_toolchains.archives[&dir.path().join("local/bin/rustc")];
        let running =
            |unpack: watch::Receiver<UnpackState>| matches!(*unpack.borrow(), UnpackState::Running);
        assert!(!running(worker_toolchains.unpack(archive, None)));
        let (_, failed_a_while_ago) = watch::channel(UnpackState::Failed {
            at: Instant::now() - RETRY_AFTER,
        });
        (worker_toolchains.unpacks.lock().unwrap())
            .insert((archive.path.clone(), None), failed_a_while_ago);
        assert!(running(worker_toolchains.unpack(archive, None)));
    }

    #[tokio::test]
    async fn test_a_compile_that_doesnt_wait_finds_the_archive_unpacking() {
        let dir = tempfile::tempdir().unwrap();
        let local = local_toolchain(&local_sysroot(dir.path(), b"std"));
        let worker_toolchains =
            worker_toolchains(dir.path(), archive(dir.path()), "/opt/toolchain/bin/rustc");
        assert_eq!(
            Err(CompileLocallyReason::UnpackingWorkerToolchain),
            worker_toolchains.toolchain(&local, Instant::now()).await
        );
        assert!(
            worker_toolchains
                .toolchain(&local, in_secs(10))
                .await
                .unwrap()
                .is_some()
        );
    }
}
