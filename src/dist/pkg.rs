// Copyright 2016 Mozilla Foundation
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

use crate::dist;
use fs_err as fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::str;

use crate::errors::*;

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
pub use self::toolchain_imp::*;

pub trait ToolchainPackager: Send {
    fn write_pkg(self: Box<Self>, f: fs::File) -> Result<()>;
}

pub trait InputsPackager: Send {
    fn write_inputs(self: Box<Self>, wtr: &mut dyn io::Write) -> Result<dist::PathTransformer>;
}

pub trait OutputsRepackager {
    fn repackage_outputs(self: Box<Self>, wtr: &mut dyn io::Write)
    -> Result<dist::PathTransformer>;
}

#[cfg(not(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
)))]
mod toolchain_imp {
    use super::ToolchainPackager;
    use fs_err as fs;

    use crate::errors::*;

    // Distributed client, but an unsupported platform for toolchain packaging so
    // create a failing implementation that will conflict with any others.
    impl<T: Send> ToolchainPackager for T {
        fn write_pkg(self: Box<Self>, _f: fs::File) -> Result<()> {
            bail!("Automatic packaging not supported on this platform")
        }
    }
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod toolchain_imp {
    use super::SimplifyPath;
    use crate::dist::ldd::find_ldd_libraries;
    use fs_err as fs;
    use std::collections::BTreeMap;
    use std::io::Write;
    use std::path::{Component, Path, PathBuf};
    use walkdir::WalkDir;

    use crate::errors::*;

    pub struct ToolchainPackageBuilder {
        // Put dirs and file in a deterministic order (map from tar_path -> real_path)
        dir_set: BTreeMap<PathBuf, PathBuf>,
        file_set: BTreeMap<PathBuf, PathBuf>,
        // Symlinks to add to the tar
        // These are _not_ tar safe, and must be made so before being added to the tar (see
        // `tar_safe_path`).
        symlinks: BTreeMap<PathBuf, PathBuf>,
    }

    impl ToolchainPackageBuilder {
        pub fn new() -> Self {
            ToolchainPackageBuilder {
                dir_set: BTreeMap::new(),
                file_set: BTreeMap::new(),
                symlinks: BTreeMap::new(),
            }
        }

        pub fn add_common(&mut self) -> Result<()> {
            self.add_dir(PathBuf::from("/tmp"))
        }

        pub fn add_executable_and_deps(&mut self, executable: PathBuf) -> Result<()> {
            let mut remaining = vec![executable];
            while let Some(obj_path) = remaining.pop() {
                assert!(obj_path.is_absolute());
                // If any parent directories are a symlink, resolve it first and record the link.
                // This is important because ld-linux may not be configured to look in the resolved
                // or non-resolved directory (i.e., both directories must work at runtime).
                //
                let tar_path = self.tarify_path(&obj_path)?;
                // If file already in the set, assume we've analysed all deps
                if self.file_set.contains_key(&tar_path) {
                    continue;
                }
                let ldd_libraries = find_ldd_libraries(&obj_path).with_context(|| {
                    format!("Failed to analyse {} with ldd", obj_path.display())
                })?;
                remaining.extend(ldd_libraries);
                self.file_set.insert(tar_path, obj_path);
            }
            Ok(())
        }

        pub fn add_dir(&mut self, dir_path: PathBuf) -> Result<()> {
            assert!(dir_path.is_absolute());
            if !dir_path.is_dir() {
                bail!(format!(
                    "{} was not a dir when readying for tar",
                    dir_path.to_string_lossy()
                ))
            }
            if dir_path
                .components()
                .next_back()
                .expect("asserted absolute")
                == Component::RootDir
            {
                return Ok(());
            }
            let tar_path = self.tarify_path(&dir_path)?;
            self.dir_set.insert(tar_path, dir_path);
            Ok(())
        }

        pub fn add_file(&mut self, file_path: PathBuf) -> Result<()> {
            assert!(file_path.is_absolute());
            if !file_path.is_file() {
                bail!(format!(
                    "{} was not a file when readying for tar",
                    file_path.to_string_lossy()
                ))
            }
            let tar_path = self.tarify_path(&file_path)?;
            self.file_set.insert(tar_path, file_path);
            Ok(())
        }

        pub fn add_dir_contents(&mut self, dir_path: &Path) -> Result<()> {
            // Although by not following symlinks we could break a custom
            // constructed toolchain with links everywhere, this is just a
            // best-effort auto packaging
            for entry in WalkDir::new(dir_path).follow_links(false) {
                let entry = entry?;
                let file_type = entry.file_type();
                if file_type.is_dir() {
                    continue;
                } else if file_type.is_symlink() {
                    let metadata = fs::metadata(entry.path())?;
                    if !metadata.file_type().is_file() {
                        continue;
                    }
                } else if !file_type.is_file() {
                    // Device or other oddity
                    continue;
                }
                trace!("walkdir add_file {}", entry.path().display());
                // It's either a file, or a symlink pointing to a file
                self.add_file(entry.path().to_owned())?;
            }
            Ok(())
        }

        pub fn into_compressed_tar<W: Write + Send + 'static>(self, writer: W) -> Result<()> {
            use gzp::{
                deflate::Gzip,
                par::compress::{Compression, ParCompress, ParCompressBuilder},
            };

            let ToolchainPackageBuilder {
                dir_set,
                file_set,
                symlinks,
            } = self;
            let par: ParCompress<'_, Gzip, W> = ParCompressBuilder::new()
                .compression_level(Compression::default())
                .from_writer(writer);
            let mut builder = tar::Builder::new(par);
            builder.mode(tar::HeaderMode::Deterministic);

            for (tar_path, dir_path) in dir_set {
                builder.append_dir(tar_path, dir_path)?;
            }
            for (tar_path, file_path) in file_set {
                let file = &mut fs::File::open(file_path)?;
                builder.append_file(tar_path, file.file_mut())?;
            }
            for (from_path, to_path) in symlinks {
                let mut header = tar::Header::new_gnu();
                header.set_entry_type(tar::EntryType::Symlink);
                header.set_size(0);
                // Leave `to_path` as absolute, assuming the tar will be used in a chroot-like
                // environment.
                builder.append_link(&mut header, tar_safe_path(from_path), to_path)?;
            }
            builder.finish().map_err(Into::into)
        }

        /// Simplify the path and strip the leading slash.
        ///
        /// Symlinks in the path are recorded for inclusion in the tarball.
        fn tarify_path(&mut self, path: &Path) -> Result<PathBuf> {
            SimplifyPath {
                resolved_symlinks: Some(&mut self.symlinks),
                traversed_dirs: None,
            }
            .simplify(path)
            .map(tar_safe_path)
        }
    }

    /// Strip a leading slash, if any.
    fn tar_safe_path(path: PathBuf) -> PathBuf {
        path.strip_prefix(Component::RootDir)
            .map(ToOwned::to_owned)
            .unwrap_or(path)
    }
}

fn populate_tar_header<'a>(
    header: &mut tar::Header,
    src: &Path,
    dest: &'a str,
    is_dir: bool,
) -> &'a str {
    match fs::metadata(src) {
        Ok(metadata) if !is_dir || metadata.is_dir() => {
            // TODO: if the source file is a symlink, I think this does bad things
            header.set_metadata(&metadata);
            if is_dir && let Ok(mode) = header.mode() {
                // Source directories may be read-only (e.g. `0o555` in `/nix/store` or
                // `FILE_ATTRIBUTE_READONLY` on Windows); ensure owner `rwx` so unpacking
                // child files, traversing `..`, and cleanup succeed on the server.
                header.set_mode(mode | 0o700);
            }
        }
        _ => {
            warn!(
                "Couldn't get metadata of {:?}, falling back to some defaults",
                src
            );
            let (mode, entry_type) = if is_dir {
                (0o755, tar::EntryType::dir())
            } else {
                (0o644, tar::EntryType::file())
            };
            header.set_mode(mode);
            header.set_uid(0);
            header.set_gid(0);
            header.set_mtime(0);
            header.set_size(0);
            header
                .set_device_major(0)
                .expect("expected a ustar or gnu header");
            header
                .set_device_minor(0)
                .expect("expected a ustar or gnu header");
            header.set_entry_type(entry_type);
        }
    }

    // tar-rs imposes that archive paths are relative
    assert!(dest.starts_with('/'));
    let dest = dest.trim_start_matches('/');
    assert!(!dest.starts_with('/'));
    dest
}

pub fn make_tar_header(src: &Path, dest: &str) -> io::Result<tar::Header> {
    let mut file_header = tar::Header::new_ustar();
    let dest = populate_tar_header(&mut file_header, src, dest, false);
    // `set_path` converts its argument to a Path and back to bytes on Windows, so this is
    // a bit of an inefficient round-trip. Windows path separators will also be normalised
    // to be like Unix, and the path is (now) relative so there should be no funny results
    // due to Windows
    // TODO: should really use a `set_path_str` or similar
    file_header.set_path(dest)?;
    Ok(file_header)
}

pub fn append_tar_dir<W: io::Write>(
    builder: &mut tar::Builder<W>,
    src: &Path,
    dest: &str,
) -> io::Result<()> {
    let mut dir_header = tar::Header::new_gnu();
    let dest = populate_tar_header(&mut dir_header, src, dest, true);
    builder.append_data(&mut dir_header, dest, io::empty())
}

/// Simplify a path to one without any relative components, erroring if it looks
/// like there could be any symlink complexity that means a simplified path is not
/// equivalent to the original (see the documentation of `fs::canonicalize` for an
/// example).
///
/// So why avoid resolving symlinks? Any path that we are trying to simplify has
/// (usually) been added to an archive because something will try access it, but
/// resolving symlinks (be they for the actual file or directory components) can
/// make the accessed path 'disappear' in favour of the canonical path.
pub fn simplify_path(path: &Path) -> Result<PathBuf> {
    SimplifyPath {
        resolved_symlinks: None,
        traversed_dirs: None,
    }
    .simplify(path)
}

pub fn simplify_path_and_dirs(path: &Path, traversed_dirs: &mut Vec<PathBuf>) -> Result<PathBuf> {
    SimplifyPath {
        resolved_symlinks: None,
        traversed_dirs: Some(traversed_dirs),
    }
    .simplify(path)
}

struct SimplifyPath<'a> {
    pub resolved_symlinks: Option<&'a mut std::collections::BTreeMap<PathBuf, PathBuf>>,
    pub traversed_dirs: Option<&'a mut Vec<PathBuf>>,
}

impl SimplifyPath<'_> {
    pub fn simplify(&mut self, path: &Path) -> Result<PathBuf> {
        let mut final_path = PathBuf::new();
        for component in path.components() {
            match component {
                c @ Component::RootDir | c @ Component::Prefix(_) | c @ Component::Normal(_) => {
                    final_path.push(c);
                    if self.resolved_symlinks.is_some() && final_path.is_symlink() {
                        let parent = final_path.parent().expect("symlinks have parents");
                        let link_target = final_path.read_link()?;
                        let new_final_path = self.simplify(&parent.join(&link_target))?;
                        let old_final_path =
                            std::mem::replace(&mut final_path, new_final_path.clone());
                        self.resolved_symlinks
                            .as_mut()
                            .unwrap()
                            .insert(old_final_path, new_final_path);
                    }
                }
                Component::ParentDir => {
                    // If the path is doing funny symlink traversals, just give up.
                    //
                    // This case should only occur if `resolved_symlinks` is `None`.
                    if final_path.is_symlink() {
                        bail!("Cannot handle symlinks in parent paths")
                    }
                    if let Some(traversed_dirs) = self.traversed_dirs.as_deref_mut()
                        && matches!(
                            final_path.components().next_back(),
                            Some(Component::Normal(_))
                        )
                    {
                        traversed_dirs.push(final_path.clone());
                    }
                    final_path.pop();
                }
                Component::CurDir => continue,
            }
        }
        Ok(final_path)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_simplify_path_and_dirs() {
        let root = if cfg!(windows) {
            Path::new(r"C:\src")
        } else {
            Path::new("/src")
        };

        let mut dirs = Vec::new();
        let simplified = simplify_path_and_dirs(
            &root.join("nptl/bits/../../x86/./nptl/bits/struct_mutex.rs"),
            &mut dirs,
        )
        .unwrap();
        assert_eq!(simplified, root.join("x86/nptl/bits/struct_mutex.rs"));
        assert_eq!(dirs, vec![root.join("nptl/bits"), root.join("nptl")]);

        dirs.clear();
        let simplified = simplify_path_and_dirs(&root.join("a/../b/../c.rs"), &mut dirs).unwrap();
        assert_eq!(simplified, root.join("c.rs"));
        assert_eq!(dirs, vec![root.join("a"), root.join("b")]);

        // Traversal at filesystem root should not record the root itself.
        let fs_root = if cfg!(windows) {
            Path::new(r"C:\")
        } else {
            Path::new("/")
        };
        dirs.clear();
        let simplified = simplify_path_and_dirs(&fs_root.join("../foo.rs"), &mut dirs).unwrap();
        assert_eq!(simplified, fs_root.join("foo.rs"));
        assert!(dirs.is_empty());
    }

    #[test]
    fn test_tar_header_helpers() {
        let tempdir = tempfile::Builder::new()
            .prefix("sccache_pkg_test")
            .tempdir()
            .unwrap();
        let existing_dir = tempdir.path().join("dir");
        fs::create_dir(&existing_dir).unwrap();
        let missing = tempdir.path().join("missing");

        let mut tar_bytes = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut tar_bytes);
            append_tar_dir(&mut builder, &existing_dir, "/dist/existing").unwrap();
            append_tar_dir(&mut builder, &missing, "/dist/missing").unwrap();
            let file_hdr = make_tar_header(&missing, "/dist/missing.rs").unwrap();
            assert_eq!(file_hdr.entry_type(), tar::EntryType::Regular);
            assert_eq!(file_hdr.mode().unwrap(), 0o644);
            builder.finish().unwrap();
        }

        let entries: Vec<_> = tar::Archive::new(tar_bytes.as_slice())
            .entries()
            .unwrap()
            .map(|e| {
                let e = e.unwrap();
                (
                    e.path().unwrap().into_owned(),
                    e.header().entry_type(),
                    e.header().mode().unwrap(),
                )
            })
            .collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, Path::new("dist/existing"));
        assert_eq!(entries[0].1, tar::EntryType::Directory);
        assert_eq!(entries[0].2 & 0o700, 0o700);
        assert_eq!(
            entries[1],
            (
                PathBuf::from("dist/missing"),
                tar::EntryType::Directory,
                0o755
            )
        );
    }
}
