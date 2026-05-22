import type {
  AdmissionCase,
  ArchiveFile,
  ArchiveIndexRow,
  CanonicalEvent,
  FirewallDevice,
} from '@/services/oxidelog';

export type PageKey = 'overview' | 'logs' | 'sources' | 'parser' | 'assets';

export type StreamFilter = 'all' | 'parsed' | 'partial' | 'failed';

export type DeviceRow = {
  id?: string;
  name?: string;
  device: string;
  sourceQuery?: string;
  protocol: string;
  port?: number;
  note?: string;
  enabled?: boolean;
  kind: 'managed' | 'observed';
  total: number;
  parsed: number;
  failed: number;
  lastTime: string;
};

export type DeviceFormState = {
  id?: string;
  name: string;
  host: string;
  protocol: 'UDP' | 'TCP' | 'TLS';
  port: number;
  note: string;
  enabled: boolean;
};

export type AssetRow = ArchiveFile & {
  category: 'Parquet 冷库' | 'Frozen 原始归档';
};

export type ArchiveIndexTableRow = ArchiveIndexRow & {
  key: string;
};

export type DisplayDeviceName = (source?: string | null, raw?: string | null) => string;

export type DeviceLookup = Map<string, FirewallDevice>;

export type RecentStreamRow = {
  key: string;
  time: string;
  status: CanonicalEvent['parse_status'];
  statusLabel: string;
  device: string;
  raw: string;
};

export type SourceGovernanceCounts = {
  pending: number;
  trusted: number;
  blocked: number;
};

export type AdmissionAction = (row: AdmissionCase) => Promise<void>;
