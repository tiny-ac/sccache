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

pub fn make_tar_header(src: &Path, dest: &str) -> io::Result<tar::Header> {
    let metadata_res = fs::metadata(src);

    let mut file_header = tar::Header::new_ustar();
    // TODO: test this works
    if let Ok(metadata) = metadata_res {
        // TODO: if the source file is a symlink, I think this does bad things
        file_header.set_metadata(&metadata);
    } else {
        warn!(
            "Couldn't get metadata of file {:?}, falling back to some defaults",
            src
        );
        file_header.set_mode(0o644);
        file_header.set_uid(0);
        file_header.set_gid(0);
        file_header.set_mtime(0);
        file_header
            .set_device_major(0)
            .expect("expected a ustar header");
        file_header
            .set_device_minor(0)
            .expect("expected a ustar header");
        file_header.set_entry_type(tar::EntryType::file());
    }

    // tar-rs imposes that `set_path` takes a relative path
    assert!(dest.starts_with('/'));
    let dest = dest.trim_start_matches('/');
    assert!(!dest.starts_with('/'));
    // `set_path` converts its argument to a Path and back to bytes on Windows, so this is
    // a bit of an inefficient round-trip. Windows path separators will also be normalised
    // to be like Unix, and the path is (now) relative so there should be no funny results
    // due to Windows
    // TODO: should really use a `set_path_str` or similar
    file_header.set_path(dest)?;
    Ok(file_header)
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
    }
    .simplify(path)
}

struct SimplifyPath<'a> {
    pub resolved_symlinks: Option<&'a mut std::collections::BTreeMap<PathBuf, PathBuf>>,
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
                    final_path.pop();
                }
                Component::CurDir => continue,
            }
        }
        Ok(final_path)
    }
}
