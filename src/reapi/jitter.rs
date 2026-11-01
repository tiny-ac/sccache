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

//! Random waits before calling a server again, so that the clients a server failed at once,
//! as while it restarts, don't all call it again at once.

use rand::Rng;
use std::time::Duration;

/// `wait`, made up to half shorter or longer at random.
pub(super) fn jittered(wait: Duration) -> Duration {
    wait.mul_f64(rand::thread_rng().gen_range(0.5..1.5))
}

#[cfg(test)]
pub(super) mod test {
    use super::*;

    pub(in crate::reapi) fn assert_in_jitter_range(wait: Duration, jittered: Duration) {
        assert!(
            wait / 2 <= jittered && jittered <= wait * 3 / 2,
            "{jittered:?} isn't {wait:?} jittered"
        );
    }

    /// Checks that each of `waits` is `wait` jittered, and that they don't all wait alike.
    pub(in crate::reapi) fn assert_jittered(wait: Duration, waits: &[Duration]) {
        for jittered in waits {
            assert_in_jitter_range(wait, *jittered);
        }
        let shortest = waits.iter().min().unwrap();
        let longest = waits.iter().max().unwrap();
        assert!(
            *longest - *shortest >= wait / 10,
            "the waits were alike: {waits:?}"
        );
    }

    /// How many waits a test draws. All of them fall within a tenth of the range once in 10^14
    /// runs.
    pub(in crate::reapi) const WAITS: usize = 16;

    #[test]
    fn test_waits_are_up_to_half_shorter_or_longer() {
        let wait = Duration::from_secs(10);
        let waits: Vec<_> = (0..1000).map(|_| jittered(wait)).collect();
        assert_jittered(wait, &waits);
        assert!(waits.iter().any(|jittered| *jittered < wait * 3 / 4));
        assert!(waits.iter().any(|jittered| *jittered > wait * 5 / 4));
    }
}
