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

//! Copies of a Rust sysroot without rust-src, so local compiles write the same outputs as
//! REAPI workers.

use crate::errors::*;
use std::path::{Path, PathBuf};

/// A copy of `sysroot` under `sysroot_copies_dir`, made of symlinks and without
/// `lib/rustlib/src`, so rustc names std's source paths `/rustc/<commit>/...` as a REAPI
/// worker's rustc does. An existing copy is used as found, so `sysroot_copies_dir` is created
/// readable only by the user. Old copies are left in place.
pub(crate) fn sysroot_copy(sysroot: &Path, sysroot_copies_dir: &Path) -> Result<PathBuf> {
    // The name hashes the sysroot's path and the entries of the directories the copy links
    // from, so `rustup target add` gives a new copy. Finding an existing copy only reads
    // these three small directories.
    let dirs = [
        sysroot.to_owned(),
        sysroot.join("lib"),
        sysroot.join("lib/rustlib"),
    ];
    let mut hasher = blake3::Hasher::new();
    hasher.update(sysroot.as_os_str().as_encoded_bytes());
    for dir in &dirs {
        let mut names = std::fs::read_dir(dir)
            .and_then(|entries| {
                entries
                    .map(|entry| entry.map(|entry| entry.file_name()))
                    .collect::<std::io::Result<Vec<_>>>()
            })
            .with_context(|| format!("failed to read {}", dir.display()))?;
        names.sort();
        for name in names {
            hasher.update(name.as_encoded_bytes());
            hasher.update(&[0]);
        }
    }
    let name = &hasher.finalize().to_hex()[..16];
    let copy = sysroot_copies_dir.join(name);
    if copy.join("lib/rustlib").is_dir() {
        return Ok(copy);
    }
    create_private_dir(sysroot_copies_dir)
        .with_context(|| format!("failed to create {}", sysroot_copies_dir.display()))?;
    // Made under a temporary name and renamed into place, so other processes never see a
    // half-made copy.
    let tmp = tempfile::Builder::new()
        .prefix(&format!("{name}."))
        .tempdir_in(sysroot_copies_dir)
        .with_context(|| {
            format!(
                "failed to create a directory in {}",
                sysroot_copies_dir.display()
            )
        })?;
    std::fs::create_dir_all(tmp.path().join("lib/rustlib"))
        .with_context(|| format!("failed to create {}", tmp.path().display()))?;
    link_all_but(sysroot, tmp.path(), "lib")?;
    link_all_but(&sysroot.join("lib"), &tmp.path().join("lib"), "rustlib")?;
    link_all_but(
        &sysroot.join("lib/rustlib"),
        &tmp.path().join("lib/rustlib"),
        "src",
    )?;
    // Disarms the removal on drop, since the rename moves the directory.
    let tmp = tmp.into_path();
    if let Err(e) = std::fs::rename(&tmp, &copy) {
        if let Err(e) = std::fs::remove_dir_all(&tmp) {
            debug!("REAPI: failed to remove {}: {e}", tmp.display());
        }
        // Another process may have made the copy first.
        if !copy.join("lib/rustlib").is_dir() {
            return Err(Error::new(e).context(format!("failed to make {}", copy.display())));
        }
    }
    Ok(copy)
}

#[cfg(unix)]
pub(super) fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
}

#[cfg(not(unix))]
pub(super) fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)
}

fn link_all_but(from: &Path, to: &Path, skip: &str) -> Result<()> {
    for entry in
        std::fs::read_dir(from).with_context(|| format!("failed to read {}", from.display()))?
    {
        let entry = entry.with_context(|| format!("failed to read {}", from.display()))?;
        if entry.file_name() != skip {
            let link = to.join(entry.file_name());
            symlink(&entry.path(), &link)
                .with_context(|| format!("failed to link {}", link.display()))?;
        }
    }
    Ok(())
}

#[cfg(unix)]
fn symlink(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(src, dst)
}

#[cfg(not(unix))]
fn symlink(_src: &Path, _dst: &Path) -> std::io::Result<()> {
    Err(std::io::ErrorKind::Unsupported.into())
}

#[cfg(all(test, unix))]
mod test {
    use super::*;

    const TARGET_LIB_DIR: &str = "lib/rustlib/aarch64-apple-darwin/lib";

    /// A sysroot with rust-src and one target triple, in `dir`.
    fn fake_sysroot(dir: &Path) -> PathBuf {
        let sysroot = dir.join("toolchain");
        for d in ["bin", "lib/rustlib/src/rust", TARGET_LIB_DIR] {
            std::fs::create_dir_all(sysroot.join(d)).unwrap();
        }
        std::fs::write(sysroot.join("lib/librustc_driver.so"), b"").unwrap();
        sysroot
    }

    #[test]
    fn test_sysroot_copy_has_all_but_rust_src() {
        let dir = tempfile::tempdir().unwrap();
        let sysroot = fake_sysroot(dir.path());
        let sysroot_copies_dir = dir.path().join("sysroot-copies");
        let copy = sysroot_copy(&sysroot, &sysroot_copies_dir).unwrap();
        assert!(copy.starts_with(&sysroot_copies_dir));
        assert!(copy.join("bin").is_dir());
        assert!(copy.join("lib/librustc_driver.so").is_file());
        assert!(copy.join(TARGET_LIB_DIR).is_dir());
        assert!(!copy.join("lib/rustlib/src").exists());
    }

    #[test]
    fn test_sysroot_copies_dir_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let sysroot_copies_dir = dir.path().join("sysroot-copies");
        sysroot_copy(&fake_sysroot(dir.path()), &sysroot_copies_dir).unwrap();
        let mode = std::fs::metadata(&sysroot_copies_dir)
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(0o700, mode & 0o777);
    }

    #[test]
    fn test_sysroot_copy_is_reused_or_made_again() {
        let dir = tempfile::tempdir().unwrap();
        let sysroot = fake_sysroot(dir.path());
        let sysroot_copies_dir = dir.path().join("sysroot-copies");
        let copy = sysroot_copy(&sysroot, &sysroot_copies_dir).unwrap();
        assert_eq!(copy, sysroot_copy(&sysroot, &sysroot_copies_dir).unwrap());
        std::fs::remove_dir_all(&copy).unwrap();
        assert_eq!(copy, sysroot_copy(&sysroot, &sysroot_copies_dir).unwrap());
        assert!(copy.join("lib/rustlib").is_dir());
    }

    #[test]
    fn test_an_added_target_gives_a_new_sysroot_copy() {
        let dir = tempfile::tempdir().unwrap();
        let sysroot = fake_sysroot(dir.path());
        let sysroot_copies_dir = dir.path().join("sysroot-copies");
        let copy = sysroot_copy(&sysroot, &sysroot_copies_dir).unwrap();
        let added_target_lib_dir = "lib/rustlib/wasm32-unknown-unknown/lib";
        std::fs::create_dir_all(sysroot.join(added_target_lib_dir)).unwrap();
        let new_copy = sysroot_copy(&sysroot, &sysroot_copies_dir).unwrap();
        assert_ne!(copy, new_copy);
        assert!(new_copy.join(added_target_lib_dir).is_dir());
    }
}
