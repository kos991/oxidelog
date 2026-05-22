export type ParseStatus = 'parsed' | 'partial' | 'failed';

export type CanonicalEvent = {
  event_id: string;
  ingest_time: string;
  source_addr?: string;
  device_id?: string | null;
  event_time?: string | null;
  vendor?: string | null;
  product?: string | null;
  src_ip?: string | null;
  src_port?: number | null;
  dst_ip?: string | null;
  dst_port?: number | null;
  protocol?: string | null;
  action?: string | null;
  severity?: string | null;
  raw: string;
  parse_status: ParseStatus;
  parse_error?: string | null;
};

export type SystemStatus = {
  service: string;
  auth_enabled: boolean;
  duckdb_path: string;
  parquet_dir: string;
  frozen_dir: string;
  events_total: number;
  events_parsed: number;
  events_failed: number;
  duckdb_bytes: number;
  parquet_files: number;
  parquet_bytes: number;
  frozen_files: number;
  frozen_bytes: number;
  metrics: {
    tcp_received: number;
    udp_received: number;
    udp_dropped: number;
    spool_written: number;
    events_stored: number;
    batches_stored: number;
    worker_errors: number;
  };
};

export type MinuteMetricPoint = {
  bucket_minute: string;
  total: number;
  parsed: number;
  failed: number;
  raw_bytes: number;
};

export type HourMetricPoint = {
  bucket_hour: string;
  total: number;
  parsed: number;
  failed: number;
  raw_bytes: number;
};

export type SourceMetricPoint = {
  source_addr: string;
  total: number;
  parsed: number;
  failed: number;
  raw_bytes: number;
  last_seen: string;
};

export type ParseErrorSummary = {
  reason: string;
  count: number;
};

export type ArchiveFile = {
  path: string;
  bytes: number;
};

export type ExportJob = {
  job_id: string;
  status: 'queued' | 'running' | 'completed' | 'failed' | 'expired';
  scope: string;
  format?: 'csv' | 'zst' | 'parquet';
  file_name: string;
  download_url: string;
  rows: number;
  file_bytes: number;
  error?: string | null;
  created_at: string;
  updated_at: string;
  expires_at: string;
};

export type ColdSearchResult = {
  files: number;
  scanned_lines: number;
  matched: number;
  limited: boolean;
  events: CanonicalEvent[];
};

export type UnifiedSearchRow = {
  result_source: 'hot' | 'archive' | string;
  archive_path?: string | null;
  device_name?: string | null;
  geo_region?: string | null;
  src_geo_region?: string | null;
  dst_geo_region?: string | null;
  event: CanonicalEvent;
};

export type ArchiveIndexRow = {
  archive_path: string;
  day: string;
  source_addr: string;
  bytes: number;
  line_count: number;
  first_seen?: string | null;
  last_seen?: string | null;
  indexed_at: string;
};

export type IpRegionInfo = {
  ip: string;
  region?: string | null;
  country?: string | null;
  province?: string | null;
  city?: string | null;
  isp?: string | null;
  raw?: string | null;
};

export type CustomIpRegion = {
  id: string;
  cidr: string;
  name: string;
  note?: string;
  enabled: boolean;
  created_at: string;
};

export type CustomIpRegionInput = {
  cidr: string;
  name: string;
  note?: string;
  enabled?: boolean;
};

export type FirewallDevice = {
  id: string;
  name: string;
  host: string;
  protocol: 'UDP' | 'TCP' | 'TLS';
  port: number;
  note?: string;
  enabled: boolean;
  created_at: string;
};

export type FirewallDeviceInput = {
  name: string;
  host: string;
  protocol: 'UDP' | 'TCP' | 'TLS';
  port: number;
  note?: string;
  enabled?: boolean;
};

export type AdmissionCase = {
  case_id: string;
  state: 'pending' | 'trusted' | 'blocked' | string;
  fingerprint_hash: string;
  network_hash?: string;
  payload_hash?: string;
  source_addr: string;
  source_ip: string;
  transport: string;
  listen_port: number;
  vendor_hint?: string | null;
  common_profile?: string | null;
  score: number;
  reason: string;
  first_seen: string;
  last_seen: string;
  seen_count: number;
  sample_paths: string;
};

export type DeviceProfile = {
  device_id: string;
  state: 'trusted' | 'blocked' | string;
  fingerprint_hash: string;
  network_hash: string;
  payload_hash: string;
  source_ip: string;
  transport: string;
  listen_port: number;
  vendor_hint?: string | null;
  common_profile?: string | null;
  approved_at?: string | null;
  approved_by?: string | null;
  updated_at: string;
};

export async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(path, {
    headers: {
      Accept: 'application/json',
    },
  });
  if (!response.ok) {
    throw new Error(`${path} 杩斿洖 ${response.status}`);
  }
  return response.json();
}

export async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(path, {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    throw new Error(`${path} 杩斿洖 ${response.status}`);
  }
  return response.json();
}

export async function postText(path: string, body: unknown): Promise<string> {
  const response = await fetch(path, {
    method: 'POST',
    headers: {
      Accept: 'text/plain, application/json',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    throw new Error(`${path} 返回 ${response.status}`);
  }
  return response.text();
}

export async function putJson<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(path, {
    method: 'PUT',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    throw new Error(`${path} 杩斿洖 ${response.status}`);
  }
  return response.json();
}

export async function deleteJson(path: string): Promise<void> {
  const response = await fetch(path, {
    method: 'DELETE',
    headers: {
      Accept: 'application/json',
    },
  });
  if (!response.ok) {
    throw new Error(`${path} 杩斿洖 ${response.status}`);
  }
}

export function fetchStatus() {
  return getJson<SystemStatus>('/api/system/status');
}

export function fetchMinuteMetrics(hours = 24, limit = 1440) {
  const query = new URLSearchParams({ hours: String(hours), limit: String(limit) });
  return getJson<MinuteMetricPoint[]>(`/api/metrics/minutes?${query.toString()}`);
}

export function fetchHourMetrics(hours = 24 * 365, limit = 24 * 365) {
  const query = new URLSearchParams({ hours: String(hours), limit: String(limit) });
  return getJson<HourMetricPoint[]>(`/api/metrics/hours?${query.toString()}`);
}

export function fetchSourceMetrics(hours = 24, limit = 200) {
  const query = new URLSearchParams({ hours: String(hours), limit: String(limit) });
  return getJson<SourceMetricPoint[]>(`/api/metrics/sources?${query.toString()}`);
}

export function fetchParserSummary() {
  return getJson<ParseErrorSummary[]>('/api/parser/summary');
}

export function fetchEvents(limit = 200, params?: Record<string, string>) {
  const query = new URLSearchParams({ limit: String(limit), ...(params || {}) });
  return getJson<CanonicalEvent[]>(`/api/events?${query.toString()}`);
}

export function eventsExportUrl(limit = 100000, params?: Record<string, string>) {
  const query = new URLSearchParams({ limit: String(limit), ...(params || {}) });
  return `/api/events/export.csv?${query.toString()}`;
}

export function searchExportUrl(limit = 1000000, params?: Record<string, string>) {
  const query = new URLSearchParams({ scope: 'all', limit: String(limit), ...(params || {}) });
  return `/api/search/export.csv?${query.toString()}`;
}

export function createExportJob(params: Record<string, string>) {
  return postJson<ExportJob>('/api/export/jobs', params);
}

export function fetchExportJobs() {
  return getJson<ExportJob[]>('/api/export/jobs');
}

export function fetchExportJob(id: string) {
  return getJson<ExportJob>(`/api/export/jobs/${encodeURIComponent(id)}`);
}

export function fetchIpRegion(ip: string) {
  const query = new URLSearchParams({ ip });
  return getJson<IpRegionInfo>(`/api/ip/region?${query.toString()}`);
}

export function fetchCustomIpRegions() {
  return getJson<CustomIpRegion[]>('/api/ip/regions/custom');
}

export function createCustomIpRegion(input: CustomIpRegionInput) {
  return postJson<CustomIpRegion>('/api/ip/regions/custom', input);
}

export function updateCustomIpRegion(id: string, input: CustomIpRegionInput) {
  return putJson<CustomIpRegion>(`/api/ip/regions/custom/${encodeURIComponent(id)}`, input);
}

export function deleteCustomIpRegion(id: string) {
  return deleteJson(`/api/ip/regions/custom/${encodeURIComponent(id)}`);
}

export function fetchDevices() {
  return getJson<FirewallDevice[]>('/api/devices');
}

export function createDevice(input: FirewallDeviceInput) {
  return postJson<FirewallDevice>('/api/devices', input);
}

export function updateDevice(id: string, input: FirewallDeviceInput) {
  return putJson<FirewallDevice>(`/api/devices/${encodeURIComponent(id)}`, input);
}

export function deleteDevice(id: string) {
  return deleteJson(`/api/devices/${encodeURIComponent(id)}`);
}

export function backfillDevices() {
  return postJson<{ updated: number }>('/api/devices/backfill', {});
}

export function fetchArchiveFiles() {
  return getJson<ArchiveFile[]>('/api/archive/files');
}

export function fetchFrozenFiles() {
  return getJson<ArchiveFile[]>('/api/archive/frozen');
}

export function fetchArchiveDays() {
  return getJson<string[]>('/api/archive/days');
}

export function searchCold(params: Record<string, string>) {
  const query = new URLSearchParams(params);
  return getJson<ColdSearchResult>(`/api/cold/search?${query.toString()}`);
}

export function searchUnified(params: Record<string, string>) {
  const query = new URLSearchParams(params);
  return getJson<UnifiedSearchRow[]>(`/api/search?${query.toString()}`);
}

export function fetchArchiveIndex(params?: Record<string, string>) {
  const query = new URLSearchParams(params || {});
  const suffix = query.toString() ? `?${query.toString()}` : '';
  return getJson<ArchiveIndexRow[]>(`/api/archive/index${suffix}`);
}

export function rebuildArchiveIndex() {
  return postJson<{ indexed: number }>('/api/archive/index/rebuild', {});
}

export function admissionCases() {
  return getJson<AdmissionCase[]>('/api/admission/cases?limit=200');
}

export function admissionProfiles() {
  return getJson<DeviceProfile[]>('/api/admission/profiles');
}

export function approveAdmissionCase(caseId: string, deviceId: string, approvedBy = 'web') {
  return postJson<DeviceProfile>(`/api/admission/cases/${encodeURIComponent(caseId)}/approve`, {
    device_id: deviceId,
    approved_by: approvedBy,
  });
}

export function blockAdmissionCase(caseId: string) {
  return postText(`/api/admission/cases/${encodeURIComponent(caseId)}/block`, {});
}

export function reopenAdmissionCase(caseId: string) {
  return postText(`/api/admission/cases/${encodeURIComponent(caseId)}/reopen`, {});
}

// ============================================================
// 自适应解析器引擎
// ============================================================

export type AdaptiveRuleStatus = 'shadow' | 'shadow_recovering' | 'active' | 'disabled';

export type AdaptiveRule = {
  rule_id: string;
  scope_key: string;
  raw_key: string;
  canonical_field: string;
  value_type: string;
  status: AdaptiveRuleStatus;
  confidence: number;
  wins: number;
  sample_count: number;
  created_at: string;
  activated_at?: string | null;
  disabled_at?: string | null;
  disabled_reason?: string | null;
  recovery_sample_rate?: number | null;
  recovery_attempts?: number;
  last_recovery_at?: string | null;
};

export type ParserDiagnostic = {
  fingerprint: string;
  scope_key?: string | null;
  reason: string;
  sample_raw: string;
  sample_raw_truncated: boolean;
  count: number;
  suggested_rule_id?: string | null;
  last_seen: string;
};

export type ParserProfile = {
  scope_key: string;
  parser_id: string;
  parser_name: string;
  success_count: number;
  partial_count: number;
  fail_count: number;
  last_seen: string;
  priority_boost: number;
};

export type ParserScopeState = {
  scope_key: string;
  source_high_entropy: boolean;
  adaptive_learning_enabled: boolean;
  unknown_source_bucket: boolean;
  metrics_gap: boolean;
  metrics_gap_since?: string | null;
  malformed_flood_until?: string | null;
  shadow_rule_cooldown_until?: string | null;
  adaptive_quarantine_until?: string | null;
  quarantine_backoff_seconds: number;
  quarantine_attempts: number;
  last_state_change: string;
  last_seen: string;
};

export type ParserCheckpointVersion = {
  snapshot_version: number;
  created_at: string;
  published_at?: string | null;
  status: 'pending' | 'published' | 'failed';
  profiles_count: number;
  rules_count: number;
  diagnostics_count: number;
  scope_state_count: number;
  aliases_count: number;
};

export type SourceDeviceAlias = {
  source_key: string;
  raw_source_addr: string;
  device_id: string;
  first_seen: string;
  last_seen: string;
  confidence: number;
};

export type AdaptiveRulesResponse = {
  rules: AdaptiveRule[];
  total: number;
};

export type ParserDiagnosticsResponse = {
  diagnostics: ParserDiagnostic[];
  total: number;
};

export type ParserProfilesResponse = {
  profiles: ParserProfile[];
  total: number;
};

export type ParserScopesResponse = {
  scopes: ParserScopeState[];
  total: number;
};

export function fetchAdaptiveRules(params?: {
  scope?: string;
  status?: string;
  page?: number;
  page_size?: number;
}) {
  const qs = new URLSearchParams();
  if (params?.scope) qs.set('scope', params.scope);
  if (params?.status) qs.set('status', params.status);
  if (params?.page) qs.set('page', String(params.page));
  if (params?.page_size) qs.set('page_size', String(params.page_size));
  const suffix = qs.toString() ? `?${qs.toString()}` : '';
  return getJson<AdaptiveRulesResponse>(`/api/parser/adaptive/rules${suffix}`);
}

export function enableAdaptiveRule(ruleId: string) {
  return postText(`/api/parser/adaptive/rules/${encodeURIComponent(ruleId)}/enable`, {});
}

export function disableAdaptiveRule(ruleId: string) {
  return postText(`/api/parser/adaptive/rules/${encodeURIComponent(ruleId)}/disable`, {});
}

export function fetchParserDiagnostics(params?: {
  scope?: string;
  page?: number;
  page_size?: number;
}) {
  const qs = new URLSearchParams();
  if (params?.scope) qs.set('scope', params.scope);
  if (params?.page) qs.set('page', String(params.page));
  if (params?.page_size) qs.set('page_size', String(params.page_size));
  const suffix = qs.toString() ? `?${qs.toString()}` : '';
  return getJson<ParserDiagnosticsResponse>(`/api/parser/diagnostics${suffix}`);
}

export function fetchParserProfiles(params?: {
  scope?: string;
  parser_id?: string;
  page?: number;
  page_size?: number;
}) {
  const qs = new URLSearchParams();
  if (params?.scope) qs.set('scope', params.scope);
  if (params?.parser_id) qs.set('parser_id', params.parser_id);
  if (params?.page) qs.set('page', String(params.page));
  if (params?.page_size) qs.set('page_size', String(params.page_size));
  const suffix = qs.toString() ? `?${qs.toString()}` : '';
  return getJson<ParserProfilesResponse>(`/api/parser/profiles${suffix}`);
}

export function fetchParserScopes(params?: {
  page?: number;
  page_size?: number;
}) {
  const qs = new URLSearchParams();
  if (params?.page) qs.set('page', String(params.page));
  if (params?.page_size) qs.set('page_size', String(params.page_size));
  const suffix = qs.toString() ? `?${qs.toString()}` : '';
  return getJson<ParserScopesResponse>(`/api/parser/scopes${suffix}`);
}
