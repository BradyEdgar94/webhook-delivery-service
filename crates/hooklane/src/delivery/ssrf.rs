//! SSRF (server-side request forgery) protection.
//!
//! **The threat.** Customers choose their webhook URL. An attacker could
//! register `http://169.254.169.254/latest/meta-data/` (cloud credentials),
//! `http://localhost:5432`, or any internal service, and our worker, which
//! runs *inside* our network, would make the request for them.
//!
//! **Why check the resolved IP and not the URL text.** `https://evil.example`
//! can resolve to `10.0.0.5`. Worse, with *DNS rebinding* the name resolves to
//! a public IP when the endpoint is created (passing validation) and to a
//! private IP later when we deliver. So the decisive check happens at
//! connection time, on the exact addresses we are about to connect to: we plug
//! a custom DNS resolver ([`SafeResolver`]) into the HTTP client that drops any
//! disallowed address. If nothing allowed is left, the connection fails.
//!
//! **Literal IPs** (`https://10.0.0.5/`) never go through DNS, so
//! [`check_url`] inspects them explicitly before every send.
//!
//! **Also enforced elsewhere:** no redirects are followed (a public URL could
//! 302 to an internal one; see `http.rs`), no proxy is used (a proxy would do
//! the DNS lookup for us, skipping our resolver), and `https://` is required
//! unless `ALLOW_HTTP=true`.
//!
//! `ALLOW_PRIVATE_ENDPOINTS=true` turns the IP rules off. It exists only for
//! local Docker networks where the fake endpoints have private addresses.
//!
//! Rejected alternative: a separate egress proxy such as Smokescreen. It is a
//! fine design but one more service to run; the in-process check gives the
//! same protection for this system.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::OnceLock;

use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use url::{Host, Url};

/// Returned (inside reqwest's error chain) when every resolved address for a
/// host is disallowed. `http.rs` looks for this type to classify the attempt.
#[derive(Debug, thiserror::Error)]
#[error("blocked by SSRF protection: {0}")]
pub struct SsrfBlocked(pub String);

/// The URL is unusable as a webhook target.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum UrlError {
    #[error("URL is not valid")]
    Invalid,
    #[error("URL must use https (http is only allowed when ALLOW_HTTP=true)")]
    SchemeNotAllowed,
    #[error("URL must not contain credentials")]
    HasCredentials,
    #[error("URL host is a blocked address: {0}")]
    BlockedAddress(String),
    #[error("URL host is not allowed: {0}")]
    BlockedHost(String),
}

#[derive(Debug, Clone, Copy)]
pub struct Policy {
    pub allow_http: bool,
    pub allow_private: bool,
}

const BLOCKED_V4: &[&str] = &[
    "0.0.0.0/8",       // "this network"
    "10.0.0.0/8",      // private
    "100.64.0.0/10",   // carrier-grade NAT
    "127.0.0.0/8",     // loopback
    "169.254.0.0/16",  // link-local, includes cloud metadata 169.254.169.254
    "172.16.0.0/12",   // private
    "192.0.0.0/24",    // IETF protocol assignments
    "192.0.2.0/24",    // TEST-NET-1
    "192.168.0.0/16",  // private
    "198.18.0.0/15",   // benchmarking
    "198.51.100.0/24", // TEST-NET-2
    "203.0.113.0/24",  // TEST-NET-3
    "224.0.0.0/4",     // multicast
    "240.0.0.0/4",     // reserved, includes broadcast 255.255.255.255
];

const BLOCKED_V6: &[&str] = &[
    "::/128",        // unspecified
    "::1/128",       // loopback
    "fc00::/7",      // unique local (private)
    "fe80::/10",     // link-local
    "ff00::/8",      // multicast
    "2001:db8::/32", // documentation
];

fn blocked_v4() -> &'static Vec<Ipv4Net> {
    static NETS: OnceLock<Vec<Ipv4Net>> = OnceLock::new();
    NETS.get_or_init(|| BLOCKED_V4.iter().filter_map(|s| s.parse().ok()).collect())
}

fn blocked_v6() -> &'static Vec<Ipv6Net> {
    static NETS: OnceLock<Vec<Ipv6Net>> = OnceLock::new();
    NETS.get_or_init(|| BLOCKED_V6.iter().filter_map(|s| s.parse().ok()).collect())
}

fn is_blocked_v4(ip: Ipv4Addr) -> bool {
    blocked_v4().iter().any(|net| net.contains(&ip))
}

/// The IPv4 address hidden inside an IPv6 address, if it is one of the forms
/// that routes to IPv4: IPv4-mapped (`::ffff:a.b.c.d`), IPv4-compatible
/// (`::a.b.c.d`, deprecated) or NAT64 (`64:ff9b::a.b.c.d`). Without this,
/// `::ffff:127.0.0.1` would sneak past the IPv4 rules.
fn embedded_v4(ip: Ipv6Addr) -> Option<Ipv4Addr> {
    if let Some(v4) = ip.to_ipv4_mapped() {
        return Some(v4);
    }
    let seg = ip.segments();
    let tail = Ipv4Addr::new(
        (seg[6] >> 8) as u8,
        seg[6] as u8,
        (seg[7] >> 8) as u8,
        seg[7] as u8,
    );
    // NAT64 well-known prefix 64:ff9b::/96
    if seg[0] == 0x64 && seg[1] == 0xff9b && seg[2..6].iter().all(|s| *s == 0) {
        return Some(tail);
    }
    // IPv4-compatible ::a.b.c.d (but not :: or ::1, handled by the v6 list)
    if seg[0..6].iter().all(|s| *s == 0) && (seg[6] != 0 || seg[7] > 1) {
        return Some(tail);
    }
    None
}

/// Is this address one we must never connect to?
pub fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_blocked_v4(v4),
        IpAddr::V6(v6) => {
            if blocked_v6().iter().any(|net| net.contains(&v6)) {
                return true;
            }
            match embedded_v4(v6) {
                Some(v4) => is_blocked_v4(v4),
                None => false,
            }
        }
    }
}

/// Validate a URL before using it: scheme, credentials, and (if the host is a
/// literal IP or an obviously-internal name) the address rules. Runs when an
/// endpoint is created/updated *and* before every send.
pub fn check_url(raw: &str, policy: Policy) -> Result<Url, UrlError> {
    let url = Url::parse(raw).map_err(|_| UrlError::Invalid)?;
    match url.scheme() {
        "https" => {}
        "http" if policy.allow_http => {}
        _ => return Err(UrlError::SchemeNotAllowed),
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(UrlError::HasCredentials);
    }
    let host = url.host().ok_or(UrlError::Invalid)?;
    if policy.allow_private {
        return Ok(url);
    }
    match host {
        Host::Ipv4(ip) if is_blocked_ip(IpAddr::V4(ip)) => {
            Err(UrlError::BlockedAddress(ip.to_string()))
        }
        Host::Ipv6(ip) if is_blocked_ip(IpAddr::V6(ip)) => {
            Err(UrlError::BlockedAddress(ip.to_string()))
        }
        Host::Domain(name) => {
            let name = name.trim_end_matches('.').to_ascii_lowercase();
            // Cheap early rejection of names that are internal by definition.
            // The resolver check at send time is still what really protects us.
            if name == "localhost" || name.ends_with(".localhost") || name.ends_with(".internal") {
                Err(UrlError::BlockedHost(name))
            } else {
                Ok(url.clone())
            }
        }
        _ => Ok(url.clone()),
    }
}

/// A DNS resolver for `reqwest` that removes disallowed addresses from every
/// lookup result. This is the connect-time check that defeats DNS rebinding.
pub struct SafeResolver {
    pub allow_private: bool,
}

impl reqwest::dns::Resolve for SafeResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let allow_private = self.allow_private;
        let host = name.as_str().to_string();
        Box::pin(async move {
            // Port 0 is a placeholder; reqwest substitutes the real port.
            let addrs: Vec<SocketAddr> =
                tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            let allowed: Vec<SocketAddr> = addrs
                .into_iter()
                .filter(|a| allow_private || !is_blocked_ip(a.ip()))
                .collect();
            if allowed.is_empty() {
                let err: Box<dyn std::error::Error + Send + Sync> = Box::new(SsrfBlocked(format!(
                    "{host} resolves only to blocked addresses"
                )));
                return Err(err);
            }
            let iter: reqwest::dns::Addrs = Box::new(allowed.into_iter());
            Ok(iter)
        })
    }
}

/// Does this error (or anything in its source chain) come from SSRF blocking?
pub fn is_ssrf_error(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = current {
        if e.downcast_ref::<SsrfBlocked>().is_some() {
            return true;
        }
        current = e.source();
    }
    false
}

/// Parse helper for tests and validation: is `s` inside one of the blocked nets?
pub fn blocked_networks() -> Vec<IpNet> {
    blocked_v4()
        .iter()
        .map(|n| IpNet::V4(*n))
        .chain(blocked_v6().iter().map(|n| IpNet::V6(*n)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const STRICT: Policy = Policy {
        allow_http: false,
        allow_private: false,
    };

    fn blocked(s: &str) -> bool {
        is_blocked_ip(s.parse().unwrap())
    }

    #[test]
    fn every_listed_ipv4_range_is_blocked_at_both_edges() {
        for net in blocked_v4() {
            assert!(is_blocked_v4(net.network()), "{net} first");
            assert!(is_blocked_v4(net.broadcast()), "{net} last");
        }
        assert_eq!(blocked_v4().len(), BLOCKED_V4.len(), "every range parsed");
    }

    #[test]
    fn every_listed_ipv6_range_is_blocked() {
        for net in blocked_v6() {
            assert!(is_blocked_ip(IpAddr::V6(net.network())), "{net} first");
            assert!(is_blocked_ip(IpAddr::V6(net.broadcast())), "{net} last");
        }
        assert_eq!(blocked_v6().len(), BLOCKED_V6.len(), "every range parsed");
    }

    #[test]
    fn specific_dangerous_addresses() {
        for ip in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "0.0.0.0",
            "255.255.255.255",
            "224.0.0.1",
            "::1",
            "::",
            "fd00::1",
            "fe80::1",
            "ff02::1",
            "2001:db8::1",
        ] {
            assert!(blocked(ip), "{ip} should be blocked");
        }
    }

    #[test]
    fn public_addresses_are_allowed() {
        for ip in [
            "8.8.8.8",
            "1.1.1.1",
            "172.32.0.1",
            "172.15.255.255",
            "100.128.0.1",
            "2606:4700:4700::1111",
            "93.184.216.34",
        ] {
            assert!(!blocked(ip), "{ip} should be allowed");
        }
    }

    #[test]
    fn ipv4_mapped_and_nat64_addresses_are_unwrapped() {
        assert!(blocked("::ffff:127.0.0.1"));
        assert!(blocked("::ffff:10.0.0.1"));
        assert!(blocked("::ffff:169.254.169.254"));
        assert!(blocked("64:ff9b::10.0.0.1"));
        assert!(blocked("64:ff9b::127.0.0.1"));
        assert!(blocked("::127.0.0.1")); // IPv4-compatible
        assert!(!blocked("::ffff:8.8.8.8"));
        assert!(!blocked("64:ff9b::8.8.8.8"));
    }

    #[test]
    fn literal_ip_urls_are_checked() {
        assert_eq!(
            check_url("https://127.0.0.1/hook", STRICT),
            Err(UrlError::BlockedAddress("127.0.0.1".into()))
        );
        assert!(matches!(
            check_url("https://169.254.169.254/latest/meta-data", STRICT),
            Err(UrlError::BlockedAddress(_))
        ));
        assert!(matches!(
            check_url("https://[::1]/x", STRICT),
            Err(UrlError::BlockedAddress(_))
        ));
        assert!(matches!(
            check_url("https://[::ffff:10.0.0.1]/x", STRICT),
            Err(UrlError::BlockedAddress(_))
        ));
        // Decimal / hex IPv4 forms are normalised by the URL parser, so they are caught too.
        assert!(matches!(
            check_url("https://2130706433/x", STRICT),
            Err(UrlError::BlockedAddress(_))
        ));
        assert!(matches!(
            check_url("https://0x7f.0.0.1/x", STRICT),
            Err(UrlError::BlockedAddress(_))
        ));
        assert!(check_url("https://8.8.8.8/hook", STRICT).is_ok());
    }

    #[test]
    fn scheme_rules() {
        assert_eq!(
            check_url("http://example.com/", STRICT),
            Err(UrlError::SchemeNotAllowed)
        );
        assert_eq!(
            check_url("ftp://example.com/", STRICT),
            Err(UrlError::SchemeNotAllowed)
        );
        assert!(check_url("https://example.com/", STRICT).is_ok());
        let allow_http = Policy {
            allow_http: true,
            allow_private: false,
        };
        assert!(check_url("http://example.com/", allow_http).is_ok());
        assert!(check_url("file:///etc/passwd", allow_http).is_err());
    }

    #[test]
    fn internal_names_and_credentials_rejected() {
        assert!(matches!(
            check_url("https://localhost/x", STRICT),
            Err(UrlError::BlockedHost(_))
        ));
        assert!(matches!(
            check_url("https://api.localhost/x", STRICT),
            Err(UrlError::BlockedHost(_))
        ));
        assert!(matches!(
            check_url("https://metadata.google.internal/x", STRICT),
            Err(UrlError::BlockedHost(_))
        ));
        assert_eq!(
            check_url("https://user:pw@example.com/", STRICT),
            Err(UrlError::HasCredentials)
        );
        assert_eq!(check_url("not a url", STRICT), Err(UrlError::Invalid));
    }

    #[test]
    fn allow_private_disables_ip_rules_but_not_scheme_rules() {
        let p = Policy {
            allow_http: false,
            allow_private: true,
        };
        assert!(check_url("https://127.0.0.1/x", p).is_ok());
        assert!(check_url("http://127.0.0.1/x", p).is_err());
    }

    #[tokio::test]
    async fn resolver_drops_private_addresses() {
        use reqwest::dns::Resolve;
        let r = SafeResolver {
            allow_private: false,
        };
        let name: reqwest::dns::Name = "localhost".parse().unwrap();
        let err = match r.resolve(name).await {
            Ok(_) => panic!("localhost must not resolve to anything usable"),
            Err(e) => e,
        };
        assert!(is_ssrf_error(err.as_ref()));

        let permissive = SafeResolver {
            allow_private: true,
        };
        let name: reqwest::dns::Name = "localhost".parse().unwrap();
        assert!(permissive.resolve(name).await.is_ok());
    }
}
