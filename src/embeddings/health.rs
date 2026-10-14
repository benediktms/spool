//! Provider health: circuit breaker plus the auth/config block (spec 11.7, 12.4). Time comes from the
//! injected clock so cooldowns are deterministic in tests.

use super::provider::{EmbedError, EmbedErrorKind};
use parking_lot::Mutex;
use serde::Serialize;
use std::time::Duration;

const FAILURE_THRESHOLD: u32 = 3;
const BASE_COOLDOWN_MS: i64 = 30_000;
const MAX_COOLDOWN_MS: i64 = 300_000;
const DEFAULT_RATE_LIMIT_MS: i64 = 30_000;
/// A half-open probe whose caller vanished must not wedge the breaker forever.
const PROBE_STALE_MS: i64 = 60_000;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CircuitInfo {
    pub last_error: String,
    pub reopens_at_ms: Option<i64>,
    /// Paused until `reset_auth_block()` (permanent profile/configuration error).
    pub blocked: bool,
}

#[derive(Default)]
struct State {
    failures: u32,
    open_until_ms: i64,
    probing_since: Option<i64>,
    last: Option<EmbedErrorKind>,
    blocked: Option<EmbedErrorKind>,
}

#[derive(Default)]
pub struct Health(Mutex<State>);

impl Health {
    pub fn blocked(&self) -> Option<EmbedErrorKind> {
        self.0.lock().blocked
    }

    /// Fast-fail check before any permit/budget/network work.
    pub fn admit(&self, now: i64) -> Result<(), EmbedError> {
        let mut s = self.0.lock();
        if let Some(k) = s.blocked {
            let msg = if k == EmbedErrorKind::Auth {
                "hosted embedding paused after an authentication failure"
            } else {
                "hosted embedding paused after a profile/model rejection"
            };
            return Err(EmbedError::new(k, msg));
        }
        if s.open_until_ms > now {
            let wait = Duration::from_millis((s.open_until_ms - now) as u64);
            return Err(
                EmbedError::new(EmbedErrorKind::Unavailable, "provider circuit is open")
                    .retryable(true)
                    .retry_after(Some(wait)),
            );
        }
        if s.open_until_ms > 0 {
            // Half-open: exactly one probe request at a time.
            if s.probing_since.is_some_and(|t| now - t < PROBE_STALE_MS) {
                return Err(EmbedError::new(
                    EmbedErrorKind::Unavailable,
                    "provider probe in progress",
                )
                .retryable(true)
                .retry_after(Some(Duration::from_secs(1))));
            }
            s.probing_since = Some(now);
        }
        Ok(())
    }

    pub fn success(&self) {
        let mut s = self.0.lock();
        s.failures = 0;
        s.open_until_ms = 0;
        s.probing_since = None;
        s.last = None;
    }

    pub fn failure(&self, e: &EmbedError, now: i64) {
        use EmbedErrorKind::*;
        let mut s = self.0.lock();
        match e.kind {
            Auth | UnsupportedModel => {
                s.blocked = Some(e.kind);
                s.last = Some(e.kind);
            }
            RateLimited | Server | Network | Timeout | BadResponse => {
                s.failures += 1;
                s.last = Some(e.kind);
                s.probing_since = None;
                let hint = e.retry_after.map(|d| d.as_millis() as i64);
                if e.kind == RateLimited {
                    s.open_until_ms = now + hint.unwrap_or(DEFAULT_RATE_LIMIT_MS).max(1000);
                } else if s.failures >= FAILURE_THRESHOLD {
                    let shift = (s.failures - FAILURE_THRESHOLD).min(4);
                    s.open_until_ms = now
                        + (BASE_COOLDOWN_MS << shift)
                            .min(MAX_COOLDOWN_MS)
                            .max(hint.unwrap_or(0));
                }
            }
            // Budget, gate, circuit and cancellation outcomes say nothing about provider health.
            _ => {
                s.probing_since = None;
            }
        }
    }

    pub fn reset(&self) {
        *self.0.lock() = State::default();
    }

    pub fn info(&self, now: i64) -> Option<CircuitInfo> {
        let s = self.0.lock();
        if let Some(k) = s.blocked {
            return Some(CircuitInfo {
                last_error: k.as_str().into(),
                reopens_at_ms: None,
                blocked: true,
            });
        }
        (s.open_until_ms > now).then(|| CircuitInfo {
            last_error: s.last.map(|k| k.as_str()).unwrap_or("unavailable").into(),
            reopens_at_ms: Some(s.open_until_ms),
            blocked: false,
        })
    }
}
