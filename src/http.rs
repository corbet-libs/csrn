//! Optional native HTTPS port. Device/browser runtimes can provide their own port.
use crate::{
    Error, Result,
    feed::{Action, Request},
};
use cchr::AuthenticatedOrigin;
use serde::Deserialize;
use std::time::Duration;

pub struct Http {
    client: reqwest::Client,
    origin: String,
    community: String,
}
/// Produced only by the configured HTTPS transport, without following redirects.
pub struct Received {
    bytes: Vec<u8>,
    action: Action,
    origin: String,
    community: String,
}
impl Received {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    /// Stamp with the caller's trusted receive time. Only fetch responses bootstrap.
    pub fn authority(&self, now: u64, fresh_for: u64) -> Result<AuthenticatedOrigin> {
        if self.action != Action::Fetch || fresh_for == 0 || fresh_for > 300 {
            return Err(Error::Response);
        }
        #[derive(Deserialize)]
        struct Header {
            revision: u64,
            key_ring: Vec<u8>,
        }
        let header: Header = serde_json::from_slice(&self.bytes).map_err(|_| Error::Response)?;
        AuthenticatedOrigin::from_authenticated_response(
            &self.origin,
            &self.community,
            &header.key_ring,
            header.revision,
            now,
            now.checked_add(fresh_for).ok_or(Error::Exhausted)?,
        )
        .map_err(Error::Trust)
    }
}
impl Http {
    pub fn new(origin: &str, community: &str) -> Result<Self> {
        Self::with_roots(origin, community, &[])
    }
    /// Explicit operator roots supplement the public root store; TLS verification stays on.
    pub fn with_roots(origin: &str, community: &str, roots: &[Vec<u8>]) -> Result<Self> {
        // Share Charter's strict HTTPS-origin validation.
        cchr::Charter::new(origin, community).map_err(Error::Trust)?;
        let origin = url::Url::parse(origin)
            .map_err(|_| Error::Configuration)?
            .origin()
            .ascii_serialization();
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .https_only(true)
            .referer(false)
            .no_proxy()
            .user_agent("community-trust-client/1.0")
            .connect_timeout(Duration::from_secs(10));
        for der in roots {
            builder = builder.add_root_certificate(
                reqwest::Certificate::from_der(der).map_err(|_| Error::Configuration)?,
            );
        }
        let client = builder.build().map_err(|_| Error::Network)?;
        Ok(Self {
            client,
            origin,
            community: community.into(),
        })
    }
    /// Cancellation is dropping this future. The caller also fences it in Follower.
    /// No bearer, cookies, member fields, arbitrary URLs or automatic retries.
    pub async fn execute(&self, request: Request, now: u64) -> Result<Received> {
        let remaining = request
            .deadline()
            .checked_sub(now)
            .filter(|v| *v > 0)
            .ok_or(Error::Timeout)?;
        let limit = match request.action() {
            Action::Fetch => cchr::MAX_FEED_BYTES,
            Action::Poll { .. } => 1024,
        };
        let mut response = self
            .client
            .post(format!("{}{}", self.origin, request.path()))
            .header("Cache-Control", "no-store")
            .header("Accept", "application/json")
            .json(&request.body())
            .timeout(Duration::from_secs(remaining))
            .send()
            .await
            .map_err(|_| Error::Network)?;
        if !response.status().is_success()
            || response.content_length().is_some_and(|v| v > limit as u64)
        {
            return Err(Error::Response);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| Error::Network)? {
            if chunk.len() > limit.saturating_sub(bytes.len()) {
                return Err(Error::Response);
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(Received {
            bytes,
            action: request.action(),
            origin: self.origin.clone(),
            community: self.community.clone(),
        })
    }
}
