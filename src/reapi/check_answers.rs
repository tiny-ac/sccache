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

//! The workers' answers to toolchain checks, kept across sccache restarts so that a new
//! sccache server doesn't run every check on a worker again. Servers don't cache the checks'
//! results, since the check actions set `do_not_cache`.

use super::digest;
use super::proto::build::bazel::remote::execution::v2 as remexec;
use crate::errors::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// How long a saved answer stays in use. A change on the workers goes unnoticed for that long,
/// as it does while one sccache server runs.
const MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone, Serialize, Deserialize)]
struct CheckAnswer {
    stdout: String,
    /// Seconds since the Unix epoch.
    answered_at: u64,
}

/// The stdout of the toolchain checks that matched the local toolchain, by server and check
/// action. The action's digest covers the toolchain's files, the command, the salt and the
/// platform.
pub(super) struct CheckAnswers {
    answers: Mutex<HashMap<String, CheckAnswer>>,
    save_file: PathBuf,
    /// The endpoint and instance name, which go into the keys only hashed, since sccache keeps
    /// the server's host out of its logs and files.
    server: String,
    max_age: Duration,
}

impl CheckAnswers {
    /// Loads the answers in `save_file`. Starts empty if `save_file` is missing or, with a
    /// warning, unreadable.
    pub(super) fn load(save_file: PathBuf, endpoint: &str, instance_name: &str) -> Self {
        let answers = read_answers(&save_file).unwrap_or_else(|e| {
            warn!(
                "REAPI: ignoring the unreadable toolchain check answers in {}: {e:#}",
                save_file.display()
            );
            HashMap::new()
        });
        CheckAnswers {
            answers: Mutex::new(answers),
            save_file,
            server: format!("{endpoint}\0{instance_name}"),
            max_age: MAX_AGE,
        }
    }

    /// The stdout a worker of this server printed for `action`, unless it's older than a day.
    pub(super) fn get(&self, action: &remexec::Digest) -> Option<Vec<u8>> {
        let answers = self.answers.lock().unwrap();
        let answer = answers.get(&self.key(action))?;
        self.is_fresh(answer)
            .then(|| answer.stdout.clone().into_bytes())
    }

    /// Keeps `stdout` as the answer to `action`, and saves it with the answers that other
    /// sccache servers saved meanwhile. Writes a file, so call it from a blocking thread.
    pub(super) fn remember(&self, action: &remexec::Digest, stdout: &[u8]) -> Result<()> {
        // The checks print text; an answer that isn't UTF-8 runs again after a restart.
        let Ok(stdout) = String::from_utf8(stdout.to_owned()) else {
            return Ok(());
        };
        let answer = CheckAnswer {
            stdout,
            answered_at: seconds_since_epoch(SystemTime::now()),
        };
        let mut answers = self.answers.lock().unwrap();
        answers.insert(self.key(action), answer);
        if let Ok(saved) = read_answers(&self.save_file) {
            for (key, answer) in saved {
                answers.entry(key).or_insert(answer);
            }
        }
        answers.retain(|_, answer| self.is_fresh(answer));
        digest::save_json(&self.save_file, &*answers)
    }

    fn key(&self, action: &remexec::Digest) -> String {
        let key = format!("{}\0{}/{}", self.server, action.hash, action.size_bytes);
        digest::of_bytes(key.as_bytes()).hash
    }

    /// An answer from the future, after the clock went back, isn't fresh.
    fn is_fresh(&self, answer: &CheckAnswer) -> bool {
        let now = seconds_since_epoch(SystemTime::now());
        now.checked_sub(answer.answered_at)
            .is_some_and(|age| age < self.max_age.as_secs())
    }
}

fn read_answers(save_file: &std::path::Path) -> Result<HashMap<String, CheckAnswer>> {
    match std::fs::read(save_file) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(e) => Err(e.into()),
    }
}

fn seconds_since_epoch(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

#[cfg(test)]
mod test {
    use super::*;

    fn action(content: &[u8]) -> remexec::Digest {
        digest::of_bytes(content)
    }

    #[test]
    fn test_an_answer_outlives_the_sccache_server() {
        let dir = tempfile::tempdir().unwrap();
        let save_file = dir.path().join("answers.json");
        let answers = CheckAnswers::load(save_file.clone(), "grpc://farm:50051", "main");
        assert_eq!(None, answers.get(&action(b"check")));
        answers
            .remember(&action(b"check"), b"commit-hash: abc\n")
            .unwrap();
        let reloaded = CheckAnswers::load(save_file, "grpc://farm:50051", "main");
        assert_eq!(
            Some(b"commit-hash: abc\n".to_vec()),
            reloaded.get(&action(b"check"))
        );
        assert_eq!(None, reloaded.get(&action(b"another check")));
    }

    #[test]
    fn test_an_answer_belongs_to_its_server_and_instance() {
        let dir = tempfile::tempdir().unwrap();
        let save_file = dir.path().join("answers.json");
        let answers = CheckAnswers::load(save_file.clone(), "grpc://farm:50051", "main");
        answers.remember(&action(b"check"), b"answer").unwrap();
        for (endpoint, instance_name) in [("grpc://other:50051", "main"), ("grpc://farm:50051", "")]
        {
            let other = CheckAnswers::load(save_file.clone(), endpoint, instance_name);
            assert_eq!(
                None,
                other.get(&action(b"check")),
                "{endpoint} {instance_name}"
            );
        }
    }

    #[test]
    fn test_the_save_file_never_holds_the_endpoint() {
        let dir = tempfile::tempdir().unwrap();
        let save_file = dir.path().join("answers.json");
        let answers = CheckAnswers::load(save_file.clone(), "grpcs://secret-farm:443", "main");
        answers.remember(&action(b"check"), b"answer").unwrap();
        let saved = std::fs::read_to_string(save_file).unwrap();
        assert!(!saved.contains("secret-farm"), "{saved}");
    }

    #[test]
    fn test_an_old_answer_or_one_from_the_future_isnt_used() {
        let dir = tempfile::tempdir().unwrap();
        let save_file = dir.path().join("answers.json");
        let answers = CheckAnswers::load(save_file.clone(), "grpc://farm:50051", "main");
        answers.remember(&action(b"old"), b"answer").unwrap();
        answers.remember(&action(b"future"), b"answer").unwrap();
        let now = seconds_since_epoch(SystemTime::now());
        for (name, answered_at) in [("old", now - MAX_AGE.as_secs()), ("future", now + 60)] {
            let key = answers.key(&action(name.as_bytes()));
            answers
                .answers
                .lock()
                .unwrap()
                .get_mut(&key)
                .unwrap()
                .answered_at = answered_at;
        }
        assert_eq!(None, answers.get(&action(b"old")));
        assert_eq!(None, answers.get(&action(b"future")));
    }

    #[test]
    fn test_saving_keeps_the_answers_another_sccache_server_saved() {
        let dir = tempfile::tempdir().unwrap();
        let save_file = dir.path().join("answers.json");
        let first = CheckAnswers::load(save_file.clone(), "grpc://farm:50051", "main");
        let second = CheckAnswers::load(save_file.clone(), "grpc://farm:50051", "main");
        first.remember(&action(b"first"), b"1").unwrap();
        second.remember(&action(b"second"), b"2").unwrap();
        let reloaded = CheckAnswers::load(save_file, "grpc://farm:50051", "main");
        assert_eq!(Some(b"1".to_vec()), reloaded.get(&action(b"first")));
        assert_eq!(Some(b"2".to_vec()), reloaded.get(&action(b"second")));
    }

    #[test]
    fn test_an_unreadable_save_file_starts_empty() {
        let dir = tempfile::tempdir().unwrap();
        let save_file = dir.path().join("answers.json");
        std::fs::write(&save_file, b"not json").unwrap();
        let answers = CheckAnswers::load(save_file.clone(), "grpc://farm:50051", "main");
        assert_eq!(None, answers.get(&action(b"check")));
        answers.remember(&action(b"check"), b"answer").unwrap();
        let reloaded = CheckAnswers::load(save_file, "grpc://farm:50051", "main");
        assert_eq!(Some(b"answer".to_vec()), reloaded.get(&action(b"check")));
    }
}
