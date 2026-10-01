//! Unnamed, extractable trust-feed state machine. No background I/O or member data.
use crate::{Error, Result};
use cchr::{AuthenticatedOrigin, Charter};
use serde::Deserialize;

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub refresh_seconds: u64,
    pub poll_seconds: u64,
    pub request_seconds: u64,
    pub initial_backoff_seconds: u64,
    pub maximum_backoff_seconds: u64,
    pub maximum_staleness_seconds: u64,
}
impl Config {
    pub fn validate(self) -> Result<Self> {
        if self.refresh_seconds == 0
            || self.poll_seconds == 0
            || !(26..=300).contains(&self.request_seconds)
            || self.initial_backoff_seconds == 0
            || self.maximum_backoff_seconds < self.initial_backoff_seconds
            || self.maximum_backoff_seconds > 86400
            || self.maximum_staleness_seconds < self.refresh_seconds
            || self.maximum_staleness_seconds > 86400
        {
            return Err(Error::Configuration);
        }
        Ok(self)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Stopped,
    Fetching,
    Following,
    Backoff,
    Unavailable,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Fetch,
    Poll { revision: u64 },
}
/// An operation ID fences late completions after timeouts, cancellation or stop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Request {
    id: u64,
    action: Action,
    deadline: u64,
}
impl Request {
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn action(&self) -> Action {
        self.action
    }
    pub fn deadline(&self) -> u64 {
        self.deadline
    }
    pub fn path(&self) -> &'static str {
        match self.action {
            Action::Fetch => "/v1/trust_feed",
            Action::Poll { .. } => "/v1/trust_changes",
        }
    }
    /// Exactly the existing community-only cvld request; no extension/member fields.
    pub fn body(&self) -> serde_json::Value {
        match self.action {
            Action::Fetch => serde_json::json!({}),
            Action::Poll { revision } => serde_json::json!({"revision":revision}),
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Announcement {
    revision: u64,
    policy_epoch: u64,
    changed: bool,
}

pub struct Follower {
    config: Config,
    state: State,
    next_id: u64,
    pending: Option<Request>,
    high_water: u64,
    next_attempt: u64,
    next_refresh: u64,
    last_success: Option<u64>,
    retry_delay: u64,
    target_revision: u64,
    target_epoch: u64,
}
impl Follower {
    pub fn new(config: Config) -> Result<Self> {
        let config = config.validate()?;
        Ok(Self {
            config,
            state: State::Stopped,
            next_id: 0,
            pending: None,
            high_water: 0,
            next_attempt: 0,
            next_refresh: 0,
            last_success: None,
            retry_delay: config.initial_backoff_seconds,
            target_revision: 0,
            target_epoch: 0,
        })
    }
    fn observe(&mut self, now: u64) -> Result<()> {
        if now < self.high_water {
            self.pending = None;
            self.state = State::Unavailable;
            return Err(Error::ClockRegression);
        }
        self.high_water = now;
        Ok(())
    }
    fn request(&mut self, action: Action, now: u64) -> Result<Request> {
        self.next_id = self.next_id.checked_add(1).ok_or(Error::Exhausted)?;
        let r = Request {
            id: self.next_id,
            action,
            deadline: now
                .checked_add(self.config.request_seconds)
                .ok_or(Error::Exhausted)?,
        };
        self.pending = Some(r);
        self.state = match action {
            Action::Fetch => State::Fetching,
            Action::Poll { .. } => State::Following,
        };
        Ok(r)
    }
    pub fn start(&mut self, now: u64) -> Result<Request> {
        self.observe(now)?;
        if self.state != State::Stopped {
            return Err(Error::Busy);
        }
        self.last_success = None;
        self.target_revision = 0;
        self.target_epoch = 0;
        self.next_refresh = now;
        self.retry_delay = self.config.initial_backoff_seconds;
        self.request(Action::Fetch, now)
    }
    pub fn stop(&mut self) {
        self.pending = None;
        self.state = State::Stopped;
        self.last_success = None;
    }
    /// At most one operation. The caller aborts transport when its deadline is due.
    pub fn next(&mut self, revision: u64, now: u64) -> Result<Option<Request>> {
        self.observe(now)?;
        if self.state == State::Stopped {
            return Ok(None);
        }
        if let Some(pending) = self.pending {
            if now < pending.deadline {
                return Ok(None);
            }
            self.pending = None;
            self.backoff(now, 0)?;
            return Ok(None);
        }
        if now < self.next_attempt {
            return Ok(None);
        }
        let action = if now >= self.next_refresh
            || self.last_success.is_none()
            || self.target_revision > revision
        {
            Action::Fetch
        } else {
            Action::Poll { revision }
        };
        self.request(action, now).map(Some)
    }
    fn take(&mut self, id: u64, now: u64) -> Result<Request> {
        self.observe(now)?;
        let request = self.pending.ok_or(Error::Obsolete)?;
        if request.id != id {
            return Err(Error::Obsolete);
        }
        self.pending = None;
        if now >= request.deadline {
            self.backoff(now, 0)?;
            return Err(Error::Timeout);
        }
        Ok(request)
    }
    fn backoff(&mut self, now: u64, jitter: u64) -> Result<()> {
        // Caller-supplied entropy can spread retries; work remains bounded for any input.
        let extra = jitter % (self.retry_delay / 2 + 1);
        let delay = self
            .retry_delay
            .saturating_add(extra)
            .min(self.config.maximum_backoff_seconds);
        self.next_attempt = now.checked_add(delay).ok_or(Error::Exhausted)?;
        self.retry_delay = self
            .retry_delay
            .saturating_mul(2)
            .min(self.config.maximum_backoff_seconds);
        self.next_refresh = now;
        self.state = State::Backoff;
        Ok(())
    }
    pub fn failed(&mut self, id: u64, now: u64, jitter: u64) -> Result<()> {
        self.take(id, now)?;
        self.backoff(now, jitter)
    }
    /// Treat announced revisions as refresh hints, never as authenticated floors.
    pub fn on_change(&mut self, revision: u64, current_revision: u64, now: u64) -> Result<()> {
        self.observe(now)?;
        if self.state == State::Stopped {
            return Err(Error::Stopped);
        }
        if revision > current_revision {
            self.target_revision = self.target_revision.max(revision);
            // Fence a poll so a member's newer epoch need not wait for long polling.
            if self
                .pending
                .is_some_and(|p| matches!(p.action, Action::Poll { .. }))
            {
                self.pending = None;
            }
            if self.state != State::Backoff {
                self.next_attempt = now;
            }
            self.next_refresh = now;
        }
        Ok(())
    }
    /// Explicit refresh, also used when a member presents a newer policy epoch.
    pub fn refresh(&mut self, now: u64) -> Result<()> {
        self.observe(now)?;
        if self.state == State::Stopped {
            return Err(Error::Stopped);
        }
        if self
            .pending
            .is_some_and(|p| matches!(p.action, Action::Poll { .. }))
        {
            self.pending = None;
        }
        if self.state != State::Backoff {
            self.next_attempt = now;
        }
        self.next_refresh = now;
        Ok(())
    }
    /// Epoch hints cannot become verified authority or bypass retry limits.
    pub fn on_policy_epoch(&mut self, epoch: u64, current_epoch: u64, now: u64) -> Result<()> {
        self.observe(now)?;
        if self.state == State::Stopped {
            return Err(Error::Stopped);
        }
        if epoch > current_epoch {
            self.target_epoch = self.target_epoch.max(epoch);
            self.refresh(now)?;
        }
        Ok(())
    }
    pub fn feed(
        &mut self,
        id: u64,
        bytes: &[u8],
        authority: &AuthenticatedOrigin,
        charter: &mut Charter,
        now: u64,
    ) -> Result<()> {
        let request = self.take(id, now)?;
        if request.action != Action::Fetch {
            self.backoff(now, 0)?;
            return Err(Error::Response);
        }
        match charter.install(bytes, authority, now) {
            Ok(v)
                if v.revision() >= self.target_revision
                    && v.policy_epoch() >= self.target_epoch =>
            {
                self.last_success = Some(now);
                self.target_revision = 0;
                self.target_epoch = 0;
                self.next_refresh = now
                    .checked_add(self.config.refresh_seconds)
                    .ok_or(Error::Exhausted)?;
                self.next_attempt = now
                    .checked_add(self.config.poll_seconds)
                    .ok_or(Error::Exhausted)?;
                self.retry_delay = self.config.initial_backoff_seconds;
                self.state = State::Following;
                Ok(())
            }
            Ok(_) => {
                self.backoff(now, 0)?;
                Err(Error::BehindAnnouncement)
            }
            Err(e) => {
                self.backoff(now, 0)?;
                Err(Error::Trust(e))
            }
        }
    }
    pub fn announcement(&mut self, id: u64, bytes: &[u8], now: u64) -> Result<()> {
        let request = self.take(id, now)?;
        let result = (|| {
            let Action::Poll { revision } = request.action else {
                return Err(Error::Response);
            };
            if bytes.len() > 1024 {
                return Err(Error::Response);
            }
            let a: Announcement = serde_json::from_slice(bytes).map_err(|_| Error::Response)?;
            if a.policy_epoch == 0 || a.revision < revision || a.changed != (a.revision > revision)
            {
                return Err(Error::Response);
            }
            self.next_attempt = now
                .checked_add(self.config.poll_seconds)
                .ok_or(Error::Exhausted)?;
            self.state = State::Following;
            self.on_change(a.revision, revision, now)
        })();
        if result.is_err() {
            self.backoff(now, 0)?;
        }
        result
    }
    pub fn state(&self) -> State {
        self.state
    }
    /// Facade readiness derives from this freshness budget and Charter's deadline.
    pub fn permits_current(&mut self, revision: u64, epoch: u64, now: u64) -> Result<()> {
        self.observe(now)?;
        if self.state == State::Stopped
            || self.target_revision > revision
            || self.target_epoch > epoch
            || !self.last_success.is_some_and(|last| {
                now.saturating_sub(last) < self.config.maximum_staleness_seconds
            })
        {
            return Err(Error::Unavailable);
        }
        Ok(())
    }
}
