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

//! The input root, REAPI's name for the directory tree of every file a remote compile
//! reads.

use super::digest;
use super::proto::build::bazel::remote::execution::v2 as remexec;
use crate::errors::*;
use prost::Message;
use std::collections::BTreeMap;

#[derive(Debug)]
pub(crate) struct InputRootFile {
    /// The path in the input root, relative and `/`-separated.
    pub(crate) path: String,
    pub(crate) digest: remexec::Digest,
    pub(crate) is_executable: bool,
}

pub(crate) struct InputRoot {
    pub(crate) digest: remexec::Digest,
    /// The encoded `Directory` protos of the root and every directory under it.
    pub(crate) directories: Vec<(remexec::Digest, Vec<u8>)>,
}

enum Node {
    File {
        digest: remexec::Digest,
        is_executable: bool,
    },
    Directory(BTreeMap<String, Node>),
}

impl InputRoot {
    /// Builds the directory tree of `files` and `directories`, in any order. A directory that
    /// holds no file is empty in the tree. Fails if two files share a path with different
    /// digests or executable bits, if a path is both a file's and a directory's (`a` and
    /// `a/b`), or if a path has an empty, `.` or `..` component.
    pub(crate) fn build<'a>(
        files: impl IntoIterator<Item = &'a InputRootFile>,
        directories: &[String],
    ) -> Result<Self> {
        let mut root = BTreeMap::new();
        for file in files {
            add_file(&mut root, &file.path, file)?;
        }
        for directory in directories {
            add_directory(&mut root, directory)?;
        }
        let mut directories = vec![];
        let digest = tree_digest(root, &mut directories);
        Ok(InputRoot {
            digest,
            directories,
        })
    }
}

fn add_file(
    dir: &mut BTreeMap<String, Node>,
    path_in_dir: &str,
    file: &InputRootFile,
) -> Result<()> {
    match path_in_dir.split_once('/') {
        None => match dir.get(path_in_dir) {
            None => {
                check_name(path_in_dir, file)?;
                dir.insert(
                    path_in_dir.to_owned(),
                    Node::File {
                        digest: file.digest.clone(),
                        is_executable: file.is_executable,
                    },
                );
                Ok(())
            }
            // The same file listed twice.
            Some(Node::File {
                digest,
                is_executable,
            }) if *digest == file.digest && *is_executable == file.is_executable => Ok(()),
            Some(_) => bail!(
                "file {:?} conflicts with another file or directory in the input root",
                file.path
            ),
        },
        Some((dir_name, rest)) => {
            check_name(dir_name, file)?;
            let node = dir
                .entry(dir_name.to_owned())
                .or_insert_with(|| Node::Directory(BTreeMap::new()));
            match node {
                Node::Directory(subdir) => add_file(subdir, rest, file),
                Node::File { .. } => bail!(
                    "file {:?} conflicts with another file or directory in the input root",
                    file.path
                ),
            }
        }
    }
}

fn add_directory(root: &mut BTreeMap<String, Node>, path: &str) -> Result<()> {
    let mut dir = root;
    for name in path.split('/') {
        if name.is_empty() || name == "." || name == ".." {
            bail!("directory {path:?} has an empty, `.` or `..` path component");
        }
        dir = match dir
            .entry(name.to_owned())
            .or_insert_with(|| Node::Directory(BTreeMap::new()))
        {
            Node::Directory(subdir) => subdir,
            Node::File { .. } => {
                bail!("directory {path:?} conflicts with a file in the input root")
            }
        };
    }
    Ok(())
}

/// Fails on names REAPI doesn't allow in a `Directory`.
fn check_name(name: &str, file: &InputRootFile) -> Result<()> {
    if name.is_empty() || name == "." || name == ".." {
        bail!(
            "file {:?} has an empty, `.` or `..` path component",
            file.path
        );
    }
    Ok(())
}

// `BTreeMap` iterates in byte order, which for UTF-8 names is the code point order
// REAPI requires of `Directory` protos.
fn tree_digest(
    dir: BTreeMap<String, Node>,
    directories: &mut Vec<(remexec::Digest, Vec<u8>)>,
) -> remexec::Digest {
    let mut directory_proto = remexec::Directory::default();
    for (name, node) in dir {
        match node {
            Node::File {
                digest,
                is_executable,
            } => directory_proto.files.push(remexec::FileNode {
                name,
                digest: Some(digest),
                is_executable,
                node_properties: None,
            }),
            Node::Directory(subdir) => directory_proto.directories.push(remexec::DirectoryNode {
                name,
                digest: Some(tree_digest(subdir, directories)),
            }),
        }
    }
    let bytes = directory_proto.encode_to_vec();
    let digest = digest::of_bytes(&bytes);
    directories.push((digest.clone(), bytes));
    digest
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;
    use test_case::test_case;

    fn file(path: &str, content: &[u8]) -> InputRootFile {
        InputRootFile {
            path: path.to_owned(),
            digest: digest::of_bytes(content),
            is_executable: false,
        }
    }

    fn proto_digest(proto: &impl Message) -> remexec::Digest {
        digest::of_bytes(&proto.encode_to_vec())
    }

    fn file_node(name: &str, content: &[u8]) -> remexec::FileNode {
        remexec::FileNode {
            name: name.to_owned(),
            digest: Some(digest::of_bytes(content)),
            ..Default::default()
        }
    }

    #[test]
    fn test_a_directory_with_no_files_goes_into_the_input_root() {
        let input_root = InputRoot::build(&[file("a/x", b"1")], &["b/c".to_owned()]).unwrap();
        let directories: HashMap<_, _> = input_root.directories.iter().cloned().collect();
        let decode = |digest: &remexec::Digest| {
            remexec::Directory::decode(directories[digest].as_slice()).unwrap()
        };
        let root = decode(&input_root.digest);
        let names: Vec<_> = root.directories.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(vec!["a", "b"], names);
        let b = decode(root.directories[1].digest.as_ref().unwrap());
        assert_eq!("c", b.directories[0].name);
        assert_eq!(
            remexec::Directory::default(),
            decode(b.directories[0].digest.as_ref().unwrap())
        );
    }

    #[test]
    fn test_a_directory_that_holds_files_leaves_the_input_root_unchanged() {
        let files = [file("a/x", b"1")];
        assert_eq!(
            InputRoot::build(&files, &[]).unwrap().digest,
            InputRoot::build(&files, &["a".to_owned()]).unwrap().digest
        );
    }

    #[test_case("a" ; "a file's path")]
    #[test_case("a/b" ; "under a file")]
    #[test_case("c/../d" ; "with a parent component")]
    fn test_a_directory_on_a_file_or_with_dotdot_fails(directory: &str) {
        assert!(InputRoot::build(&[file("a", b"1")], &[directory.to_owned()]).is_err());
    }

    #[test]
    fn test_directories_hold_every_directory_proto() {
        let input_root = InputRoot::build(
            &[file("a/x", b"1"), file("a/y/z", b"2"), file("b", b"3")],
            &[],
        )
        .unwrap();
        let directories: HashMap<_, _> = input_root.directories.iter().cloned().collect();
        assert_eq!(3, directories.len());
        assert!(directories.contains_key(&input_root.digest));
        for (digest, bytes) in &directories {
            assert_eq!(*digest, digest::of_bytes(bytes));
            let directory = remexec::Directory::decode(bytes.as_slice()).unwrap();
            for subdir in directory.directories {
                assert!(directories.contains_key(&subdir.digest.unwrap()));
            }
        }
    }

    #[test]
    fn test_same_files_in_any_order_give_the_same_digest() {
        let input_root =
            InputRoot::build(&[file("src/lib.rs", b"a"), file("b/c/d", b"d")], &[]).unwrap();
        let reordered =
            InputRoot::build(&[file("b/c/d", b"d"), file("src/lib.rs", b"a")], &[]).unwrap();
        assert_eq!(input_root.digest, reordered.digest);
    }

    #[test]
    fn test_a_changed_file_changes_the_digest() {
        let input_root = InputRoot::build(&[file("src/lib.rs", b"a")], &[]).unwrap();
        let changed = InputRoot::build(&[file("src/lib.rs", b"b")], &[]).unwrap();
        assert_ne!(input_root.digest, changed.digest);
        let mut executable_file = file("src/lib.rs", b"a");
        executable_file.is_executable = true;
        assert_ne!(
            input_root.digest,
            InputRoot::build(&[executable_file], &[]).unwrap().digest
        );
    }

    #[test]
    fn test_digest_is_of_the_root_directory_proto() {
        let input_root =
            InputRoot::build(&[file("b", b"1"), file("a/x", b"2"), file("c", b"3")], &[]).unwrap();
        let a_proto = remexec::Directory {
            files: vec![file_node("x", b"2")],
            ..Default::default()
        };
        let expected = remexec::Directory {
            files: vec![file_node("b", b"1"), file_node("c", b"3")],
            directories: vec![remexec::DirectoryNode {
                name: "a".to_owned(),
                digest: Some(proto_digest(&a_proto)),
            }],
            ..Default::default()
        };
        assert_eq!(proto_digest(&expected), input_root.digest);
    }

    #[test]
    fn test_names_are_in_byte_order() {
        let input_root =
            InputRoot::build(&[file("a", b"1"), file("B", b"2"), file("é", b"3")], &[]).unwrap();
        let expected = remexec::Directory {
            files: vec![
                file_node("B", b"2"),
                file_node("a", b"1"),
                file_node("é", b"3"),
            ],
            ..Default::default()
        };
        assert_eq!(proto_digest(&expected), input_root.digest);
    }

    #[test]
    fn test_a_file_listed_twice_is_accepted() {
        assert!(InputRoot::build(&[file("a", b"1"), file("a", b"1")], &[]).is_ok());
    }

    #[test]
    fn test_a_file_listed_twice_with_a_different_executable_bit_is_rejected() {
        let mut executable_file = file("a", b"1");
        executable_file.is_executable = true;
        assert!(InputRoot::build(&[file("a", b"1"), executable_file], &[]).is_err());
    }

    #[test_case(&["a", "a"] ; "same path with different contents")]
    #[test_case(&["a", "a/b"] ; "file then directory")]
    #[test_case(&["a/b", "a"] ; "directory then file")]
    #[test_case(&["/a"] ; "absolute")]
    #[test_case(&["a//b"] ; "empty component")]
    #[test_case(&["a/"] ; "trailing slash")]
    #[test_case(&["a/./b"] ; "dot")]
    #[test_case(&["a/../b"] ; "dot dot")]
    fn test_conflicting_or_invalid_paths_are_rejected(paths: &[&str]) {
        let files: Vec<_> = paths
            .iter()
            .enumerate()
            .map(|(i, path)| file(path, &[i as u8]))
            .collect();
        assert!(InputRoot::build(&files, &[]).is_err());
    }
}
