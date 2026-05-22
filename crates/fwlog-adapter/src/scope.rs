use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeNormalizationMode {
    SourceIp,
    SourceIpPort,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceScope {
    pub scope_key: String,
    pub normalized_source: String,
    pub unknown_source_bucket: bool,
    pub adaptive_learning_enabled: bool,
}

pub fn normalize_source_scope(raw_source: &str, mode: ScopeNormalizationMode) -> SourceScope {
    if let Some((scheme, host, port)) = parse_scheme_host_port(raw_source) {
        let normalized = match mode {
            ScopeNormalizationMode::SourceIp => format!("{scheme}://{host}"),
            ScopeNormalizationMode::SourceIpPort => match port {
                Some(port) => format!("{scheme}://{host}:{port}"),
                None => format!("{scheme}://{host}"),
            },
        };

        return SourceScope {
            scope_key: format!("source:{normalized}"),
            normalized_source: normalized,
            unknown_source_bucket: false,
            adaptive_learning_enabled: true,
        };
    }

    let hash_prefix = unknown_hash_prefix(raw_source);
    let normalized = format!("unknown:{hash_prefix}");
    SourceScope {
        scope_key: format!("source:{normalized}"),
        normalized_source: normalized,
        unknown_source_bucket: true,
        adaptive_learning_enabled: false,
    }
}

fn parse_scheme_host_port(raw_source: &str) -> Option<(&str, String, Option<u16>)> {
    let (scheme, rest) = raw_source.split_once("://")?;
    if scheme.is_empty() || rest.is_empty() {
        return None;
    }

    let host_port = rest.split('/').next().unwrap_or(rest);
    let (host, port) = if host_port.starts_with('[') {
        let end = host_port.find(']')?;
        let host = host_port[1..end].to_string();
        let port = host_port[end + 1..]
            .strip_prefix(':')
            .and_then(|value| value.parse::<u16>().ok());
        (host, port)
    } else {
        match host_port.rsplit_once(':') {
            Some((host, port)) if !host.contains(':') => {
                (host.to_string(), port.parse::<u16>().ok())
            }
            _ => (host_port.to_string(), None),
        }
    };

    if host.is_empty() {
        None
    } else {
        Some((scheme, host, port))
    }
}

fn unknown_hash_prefix(raw_source: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_source.as_bytes());
    let digest = hasher.finalize();
    hex_prefix(&digest, 8)
}

fn hex_prefix(bytes: &[u8], nibbles: usize) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(nibbles);
    for byte in bytes {
        if out.len() == nibbles {
            break;
        }
        out.push(HEX[(byte >> 4) as usize] as char);
        if out.len() == nibbles {
            break;
        }
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_ip_mode_drops_ephemeral_ports() {
        let scope =
            normalize_source_scope("udp://192.168.1.10:55123", ScopeNormalizationMode::SourceIp);
        assert_eq!(scope.scope_key, "source:udp://192.168.1.10");
        assert_eq!(scope.normalized_source, "udp://192.168.1.10");
        assert!(!scope.unknown_source_bucket);
    }

    #[test]
    fn source_ip_port_mode_preserves_port() {
        let scope =
            normalize_source_scope("tcp://127.0.0.1:1514", ScopeNormalizationMode::SourceIpPort);
        assert_eq!(scope.scope_key, "source:tcp://127.0.0.1:1514");
    }

    #[test]
    fn malformed_sources_use_stable_unknown_hash_bucket() {
        let first = normalize_source_scope("not a uri", ScopeNormalizationMode::SourceIp);
        let second = normalize_source_scope("not a uri", ScopeNormalizationMode::SourceIp);
        let other = normalize_source_scope("also not a uri", ScopeNormalizationMode::SourceIp);

        assert!(first.scope_key.starts_with("source:unknown:"));
        assert_eq!(first.scope_key, second.scope_key);
        assert_ne!(first.scope_key, other.scope_key);
        assert!(first.unknown_source_bucket);
        assert!(!first.adaptive_learning_enabled);
    }
}
