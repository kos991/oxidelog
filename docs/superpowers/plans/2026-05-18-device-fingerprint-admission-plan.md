# OxideLog Device Fingerprint Admission Plan

Date: 2026-05-18

## Goal

Implement a "No Auth, No Entry" device fingerprint admission system. Only trusted devices may enter the parser and DuckDB hot `events` table. Unknown or blocked devices must not pollute the hot store.

## Confirmed Decisions

- Admission gate runs before the official parser.
- Pending devices write only to quarantine spool, not DuckDB hot events.
- TTL and TCP Window Size are optional phase-2 network metadata fields.
- First version relies on source address plus payload DNA.
- Common market fingerprint profiles help identify candidates, but never auto-authorize first-seen devices.
- First-seen devices always require manual approval before becoming Trusted.

## Runtime Flow

```text
TCP/UDP listener
  -> RawLog + optional PacketMeta
  -> FingerprintExtractor
  -> CommonFingerprintLibrary
  -> LocalDeviceProfileMatcher
  -> AdmissionPolicy
      -> Trusted: parse -> bind device_id -> DuckDB events
      -> Pending: quarantine spool + admission case
      -> Blocked: block audit + optional limited sample
```

## Three Admission States

### Trusted

Criteria:
- Matches a locally approved device profile.
- Source identity matches: transport + source IP + listen port.
- Payload DNA is compatible with the approved profile.
- No deny rule or severe fingerprint drift is detected.

Action:
- Allow `SangforAdapter.parse(raw)` or future vendor parser.
- Allow `DuckDbStore::insert_batch`.
- Attach `device_id` before insert.

### Pending

Criteria:
- First-seen fingerprint.
- Source address exists but payload DNA does not strongly match.
- Optional network metadata drifts mildly from the approved profile.

Action:
- Do not parse.
- Do not insert into DuckDB hot events.
- Write raw samples to quarantine spool.
- Upsert an `admission_cases` row for approval.

### Blocked

Criteria:
- Explicit deny rule.
- Rejected device fingerprint.
- Severe drift from a trusted profile.
- One source address presents conflicting vendor DNA.

Action:
- Do not parse.
- Do not insert into DuckDB hot events.
- Record block reason, count, and last seen time.
- Optionally keep a small sample set for forensics.

## Fingerprint Fields

### Network Identity

Phase 1 required:
- `source_ip`
- `transport`: `tcp` or `udp`
- `listen_port`

Phase 2 optional:
- `ttl_bucket`
- `tcp_window_bucket`
- `mss`
- `tcp_options_hash`
- `ja3`
- `ja4`

Current TCP/UDP application listeners usually cannot observe TTL and TCP Window Size. These fields should be modeled now but populated later via pcap/raw socket/eBPF or another packet metadata side channel.

### Syslog Fingerprint

- `syslog_format`: `rfc3164`, `rfc5424`, `raw_vendor`, `unknown`
- `pri_present`
- `facility`
- `severity`
- `hostname_shape`
- `app_name`
- `structured_data_present`
- `timestamp_format`
- `timezone_present`

### Payload DNA

- `encoding`: `utf8`, `gbk`, `gb18030`, `unknown`, `mixed_or_invalid`
- `bom_present`
- `static_prefixes`
- `vendor_keywords`
- `field_separator_style`
- `kv_style`: `key=value`, `key: value`, `chinese_field:value`, `csv_like`
- `message_template_hash`
- `sample_token_hash`

Example Sangfor signals:

```text
localhost nat:
日志类型:NAT日志
源地址
目的地址
转换前
转换后
```

## Hash Model

```text
network_fingerprint_hash = hash(source_ip, transport, listen_port, ttl_bucket?, tcp_window_bucket?)
payload_fingerprint_hash = hash(timestamp_format, static_prefixes, encoding, vendor_hint)
device_fingerprint_hash = hash(network_fingerprint_hash, payload_fingerprint_hash)
```

Store both hashes and expanded fields. Hashes are for matching; expanded fields are for UI, audit, and debugging.

## Common Fingerprint Library

The common library identifies candidate vendors and protocol stack hints. It must not authorize devices by itself.

Initial profile targets:
- `sangfor`
- `huawei_usg`
- `h3c_secpath`
- `ruijie`
- `hillstone`
- `fortinet_fortigate`
- `palo_alto`
- `cisco_asa`
- `juniper_srx`
- `checkpoint`
- `sophos`
- `sonicwall`

Recommended first implementation: ship only Sangfor with the framework ready for more YAML or Rust-defined profiles.

## Matching Policy

Recommended first-version scoring:

```text
source_ip + transport + listen_port: required
timestamp_format match: +20
static_prefix match: +40
encoding match: +20
vendor_hint match: +20
ttl_bucket match: +10 optional
tcp_window_bucket match: +10 optional
```

Decision:

```text
explicit deny or severe conflict -> Blocked
first-seen fingerprint -> Pending
approved local profile and score >= 80 -> Trusted
40 <= score < 80 -> Pending
score < 40 on known source -> Blocked or Pending based on drift policy
```

Important: first-seen devices never become Trusted automatically.

## Proposed Components

### New crate: `crates/fwlog-admission`

Responsibilities:
- Extract fingerprints from `RawLog` and optional packet metadata.
- Match common profiles.
- Match local approved profiles.
- Return an `AdmissionDecision`.

It should not depend on `fwlog-storage`.

Core types:

```rust
struct PacketMeta {
    ttl: Option<u8>,
    tcp_window_size: Option<u32>,
    mss: Option<u16>,
    tcp_options_hash: Option<String>,
    ja3: Option<String>,
    ja4: Option<String>,
}

enum AdmissionState {
    Trusted,
    Pending,
    Blocked,
}

struct AdmissionDecision {
    state: AdmissionState,
    device_id: Option<String>,
    reason: String,
    score: u16,
    fingerprint_hash: String,
}
```

### `fwlogd` pipeline integration

Current path:

```text
RawLog -> spool.append -> adapter.parse -> bind_device_ids -> insert_batch
```

Target path:

```text
RawLog -> admission.evaluate
  Trusted -> existing spool.append -> adapter.parse -> device_id -> insert_batch
  Pending -> quarantine.write -> admission case upsert
  Blocked -> blocked audit upsert
```

### Storage

Add admission-specific persistence, preferably in `fwlog-storage`:

```sql
CREATE TABLE device_profiles (
  device_id TEXT PRIMARY KEY,
  state TEXT NOT NULL,
  fingerprint_hash TEXT NOT NULL,
  network_hash TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  source_ip TEXT NOT NULL,
  transport TEXT NOT NULL,
  listen_port INTEGER NOT NULL,
  vendor_hint TEXT,
  common_profile TEXT,
  approved_at TEXT,
  approved_by TEXT,
  updated_at TEXT NOT NULL
);
```

```sql
CREATE TABLE admission_cases (
  case_id TEXT PRIMARY KEY,
  state TEXT NOT NULL,
  fingerprint_hash TEXT NOT NULL,
  source_addr TEXT NOT NULL,
  source_ip TEXT NOT NULL,
  transport TEXT NOT NULL,
  listen_port INTEGER NOT NULL,
  vendor_hint TEXT,
  common_profile TEXT,
  score INTEGER NOT NULL,
  reason TEXT NOT NULL,
  first_seen TEXT NOT NULL,
  last_seen TEXT NOT NULL,
  seen_count BIGINT NOT NULL,
  sample_paths TEXT NOT NULL
);
```

### API

Minimum endpoints:

```text
GET /api/admission/cases?state=pending|blocked|trusted
GET /api/admission/cases/:case_id
POST /api/admission/cases/:case_id/approve
POST /api/admission/cases/:case_id/block
POST /api/admission/cases/:case_id/reopen
GET /api/admission/profiles
```

Approve flow:

```text
Pending case -> select existing device or create device -> write device_profiles -> state Trusted
```

## Relationship with Current P1 Work

P1 is adding stable `device_id` binding and backfill. The admission system should build on that:

- Keep `devices.json` as the short-term device inventory.
- Add `device_profiles` as the admission fingerprint layer.
- On approval, map fingerprint profile to `device_id`.
- Do not rewrite P1 device binding in the same step.

## Implementation Phases

### Phase 1: MVP No Auth, No Entry, 3-5 days

Scope:
- Add `fwlog-admission` crate.
- Implement source identity and payload DNA extractor.
- Add Sangfor common profile.
- Add admission policy.
- Add quarantine spool writer.
- Add storage tables and APIs for pending/block/approve.
- Integrate `fwlogd` pipeline so only Trusted reaches parser and DuckDB hot events.
- Add unit tests and integration tests for Trusted/Pending/Blocked.

Validation:
- Unknown source does not call parser and does not insert into `events`.
- Approved source reaches parser and `events`.
- Blocked source is counted and never reaches hot DB.
- GBK/UTF-8/RFC3164/RFC5424 probes are covered by tests.

### Phase 2: Common profile expansion, 3-5 days

Scope:
- Add market profiles for common firewall vendors.
- Add drift policy and conflict explanations.
- Add better sample token/template hashing.
- Improve UI display fields if needed.

### Phase 3: Network metadata, 1-2 weeks

Scope:
- Add pcap/raw socket/eBPF or platform-specific packet metadata source.
- Populate TTL/TCP Window/MSS/TCP options.
- Add JA3/JA4 if TLS syslog or HTTPS ingestion exists.

## Risks

- TTL and TCP Window are not available from the current application-level listener.
- Approval APIs must be careful not to create duplicate device profiles for the same source.
- Quarantine spool retention must be bounded to avoid disk growth.
- Common vendor fingerprints can produce false positives and must never auto-authorize.
- This should not be mixed into P1 until P1 compiles and passes tests.

## Recommended Next Step

Finish and validate P1 first. Then implement Phase 1 admission MVP as a separate change set.
