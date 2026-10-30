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

//! Pauses action cache lookups after several fail in a row, so a server that stops answering
//! doesn't delay every compile by a lookup timeout. After a pause, one compile looks up alone
//! until the server answers it or fails it again.

use super::client::RPC_TIMEOUT;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub(super) const FAILURES_TO_PAUSE: u32 = 3;
const PAUSE_MIN: Duration = Duration::from_secs(5);
const PAUSE_MAX: Duration = Duration::from_secs(60);

#[derive(Default)]
pub(super) struct LookupPause {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    failures_in_a_row: u32,
    pauses_in_a_row: u32,
    paused_until: Option<Instant>,
    /// When the lookup that the first compile after a pause makes alone has timed out.
    probe_until: Option<Instant>,
}

impl LookupPause {
    pub(super) fn may_look_up(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        if state.paused_until.is_some_and(|until| now < until) {
            return false;
        }
        if state.pauses_in_a_row == 0 {
            return true;
        }
        // A probe that never reaches the lookup, such as one whose toolchain fails to hash,
        // gives way to another after the time a lookup can take.
        if state.probe_until.is_some_and(|until| now < until) {
            return false;
        }
        state.probe_until = Some(now + RPC_TIMEOUT);
        true
    }

    /// Ends any pause and its backoff, since the server answered.
    pub(super) fn lookup_succeeded(&self) {
        *self.state.lock().unwrap() = State::default();
    }

    /// Counts a failed lookup, and pauses lookups after `FAILURES_TO_PAUSE` in a row, or after
    /// one following a pause. Returns the pause it started, if any.
    pub(super) fn lookup_failed(&self) -> Option<Duration> {
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        // Lookups in flight when the pause started neither extend it nor count toward the
        // next one.
        if state.paused_until.is_some_and(|until| now < until) {
            return None;
        }
        state.failures_in_a_row += 1;
        if state.pauses_in_a_row == 0 && state.failures_in_a_row < FAILURES_TO_PAUSE {
            return None;
        }
        let pause = PAUSE_MIN
            .saturating_mul(2u32.saturating_pow(state.pauses_in_a_row))
            .min(PAUSE_MAX);
        *state = State {
            failures_in_a_row: 0,
            pauses_in_a_row: state.pauses_in_a_row + 1,
            paused_until: Some(now + pause),
            probe_until: None,
        };
        Some(pause)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn fail(lookup_pause: &LookupPause, times: u32) -> Option<Duration> {
        (0..times)
            .filter_map(|_| lookup_pause.lookup_failed())
            .last()
    }

    fn end_pause(lookup_pause: &LookupPause) {
        lookup_pause.state.lock().unwrap().paused_until = Some(Instant::now());
    }

    #[test]
    fn test_lookups_pause_after_failing_in_a_row() {
        let lookup_pause = LookupPause::default();
        assert_eq!(None, fail(&lookup_pause, FAILURES_TO_PAUSE - 1));
        assert!(lookup_pause.may_look_up());
        assert_eq!(Some(PAUSE_MIN), fail(&lookup_pause, 1));
        assert!(!lookup_pause.may_look_up());
    }

    #[test]
    fn test_a_successful_lookup_resets_the_count() {
        let lookup_pause = LookupPause::default();
        fail(&lookup_pause, FAILURES_TO_PAUSE - 1);
        lookup_pause.lookup_succeeded();
        assert_eq!(None, fail(&lookup_pause, FAILURES_TO_PAUSE - 1));
        assert!(lookup_pause.may_look_up());
    }

    #[test]
    fn test_failures_during_a_pause_dont_extend_it() {
        let lookup_pause = LookupPause::default();
        fail(&lookup_pause, FAILURES_TO_PAUSE);
        assert_eq!(None, fail(&lookup_pause, FAILURES_TO_PAUSE));
        end_pause(&lookup_pause);
        assert_eq!(Some(PAUSE_MIN * 2), fail(&lookup_pause, 1));
    }

    #[test]
    fn test_one_compile_looks_up_after_a_pause() {
        let lookup_pause = LookupPause::default();
        fail(&lookup_pause, FAILURES_TO_PAUSE);
        end_pause(&lookup_pause);
        assert!(lookup_pause.may_look_up());
        assert!(!lookup_pause.may_look_up(), "a second compile looked up");
        // A probe that never got to its lookup.
        lookup_pause.state.lock().unwrap().probe_until = Some(Instant::now());
        assert!(lookup_pause.may_look_up());
    }

    #[test]
    fn test_pauses_double_up_to_the_max() {
        let lookup_pause = LookupPause::default();
        let mut failures = FAILURES_TO_PAUSE;
        let pauses: Vec<_> = (0..6)
            .map(|_| {
                let pause = fail(&lookup_pause, failures).unwrap();
                end_pause(&lookup_pause);
                // After a pause, the first failure starts the next one.
                failures = 1;
                pause.as_secs()
            })
            .collect();
        assert_eq!(vec![5, 10, 20, 40, 60, 60], pauses);
    }

    #[test]
    fn test_a_successful_lookup_ends_a_pause_and_its_backoff() {
        let lookup_pause = LookupPause::default();
        fail(&lookup_pause, FAILURES_TO_PAUSE);
        // A lookup in flight when the pause started.
        lookup_pause.lookup_succeeded();
        assert!(lookup_pause.may_look_up());
        assert_eq!(Some(PAUSE_MIN), fail(&lookup_pause, FAILURES_TO_PAUSE));
    }
}
