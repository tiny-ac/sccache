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

//! Paths without `.` and `..` components, for the files sent to a remote build.

use std::path::{Component, Path, PathBuf};

use crate::errors::*;

/// Simplify a path to one without any relative components, erroring if it looks
/// like there could be any symlink complexity that means a simplified path is not
/// equivalent to the original (see the documentation of `fs::canonicalize` for an
/// example).
///
/// So why avoid resolving symlinks? Any path that we are trying to simplify has
/// (usually) been added to an archive because something will try access it, but
/// resolving symlinks (be they for the actual file or directory components) can
/// make the accessed path 'disappear' in favour of the canonical path.
#[cfg(feature = "dist-client")]
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

pub(crate) struct SimplifyPath<'a> {
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
}
