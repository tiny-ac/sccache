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

//! Pauses a kind of call to the server after several fail in a row, so a server that stops
//! answering doesn't delay every compile by a call's timeout. After a pause, one compile makes
//! the call alone until the server answers it or fails it again.

use std::sync::Mutex;
use std::time::{Duration, Instant};

pub(super) const FAILURES_TO_PAUSE: u32 = 3;
const PAUSE_MIN: Duration = Duration::from_secs(5);
const PAUSE_MAX: Duration = Duration::from_secs(60);

pub(super) struct Pause {
    state: Mutex<State>,
    call_timeout: Duration,
}

#[derive(Default)]
struct State {
    failures_in_a_row: u32,
    pauses_in_a_row: u32,
    paused_until: Option<Instant>,
    /// When the call that the first compile after a pause makes alone has timed out.
    probe_until: Option<Instant>,
}

impl Pause {
    /// A pause for calls that take up to `call_timeout`.
    pub(super) fn new(call_timeout: Duration) -> Self {
        Pause {
            state: Default::default(),
            call_timeout,
        }
    }

    pub(super) fn may_call(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        if state.paused_until.is_some_and(|until| now < until) {
            return false;
        }
        if state.pauses_in_a_row == 0 {
            return true;
        }
        // A probe that never reaches the call, such as one whose toolchain fails to hash,
        // gives way to another after the time a call can take.
        if state.probe_until.is_some_and(|until| now < until) {
            return false;
        }
        state.probe_until = Some(now + self.call_timeout);
        true
    }

    /// Ends any pause and its backoff, since the server answered.
    pub(super) fn call_succeeded(&self) {
        *self.state.lock().unwrap() = State::default();
    }

    /// Counts a failed call, and pauses calls after `FAILURES_TO_PAUSE` in a row, or after one
    /// following a pause. Returns the pause it started, if any.
    pub(super) fn call_failed(&self) -> Option<Duration> {
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        // Calls in flight when the pause started neither extend it nor count toward the next
        // one.
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

    fn pause() -> Pause {
        Pause::new(Duration::from_secs(60))
    }

    fn fail(pause: &Pause, times: u32) -> Option<Duration> {
        (0..times).filter_map(|_| pause.call_failed()).last()
    }

    fn end_pause(pause: &Pause) {
        pause.state.lock().unwrap().paused_until = Some(Instant::now());
    }

    #[test]
    fn test_calls_pause_after_failing_in_a_row() {
        let pause = pause();
        assert_eq!(None, fail(&pause, FAILURES_TO_PAUSE - 1));
        assert!(pause.may_call());
        assert_eq!(Some(PAUSE_MIN), fail(&pause, 1));
        assert!(!pause.may_call());
    }

    #[test]
    fn test_a_successful_call_resets_the_count() {
        let pause = pause();
        fail(&pause, FAILURES_TO_PAUSE - 1);
        pause.call_succeeded();
        assert_eq!(None, fail(&pause, FAILURES_TO_PAUSE - 1));
        assert!(pause.may_call());
    }

    #[test]
    fn test_failures_during_a_pause_dont_extend_it() {
        let pause = pause();
        fail(&pause, FAILURES_TO_PAUSE);
        assert_eq!(None, fail(&pause, FAILURES_TO_PAUSE));
        end_pause(&pause);
        assert_eq!(Some(PAUSE_MIN * 2), fail(&pause, 1));
    }

    #[test]
    fn test_one_compile_calls_after_a_pause() {
        let pause = pause();
        fail(&pause, FAILURES_TO_PAUSE);
        end_pause(&pause);
        assert!(pause.may_call());
        assert!(!pause.may_call(), "a second compile called");
        // A probe that never got to its call.
        pause.state.lock().unwrap().probe_until = Some(Instant::now());
        assert!(pause.may_call());
    }

    #[test]
    fn test_pauses_double_up_to_the_max() {
        let pause = pause();
        let mut failures = FAILURES_TO_PAUSE;
        let pauses: Vec<_> = (0..6)
            .map(|_| {
                let started = fail(&pause, failures).unwrap();
                end_pause(&pause);
                // After a pause, the first failure starts the next one.
                failures = 1;
                started.as_secs()
            })
            .collect();
        assert_eq!(vec![5, 10, 20, 40, 60, 60], pauses);
    }

    #[test]
    fn test_a_successful_call_ends_a_pause_and_its_backoff() {
        let pause = pause();
        fail(&pause, FAILURES_TO_PAUSE);
        // A call in flight when the pause started.
        pause.call_succeeded();
        assert!(pause.may_call());
        assert_eq!(Some(PAUSE_MIN), fail(&pause, FAILURES_TO_PAUSE));
    }
}
