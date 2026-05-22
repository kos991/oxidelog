import React from 'react';
import { Space, Tag } from 'antd';
import type { CanonicalEvent, FirewallDevice, ParseStatus } from '@/services/oxidelog';

export const fmt = (value?: string | number | null) =>
  value === undefined || value === null || value === '' ? '-' : String(value);

export const bytes = (input?: number) => {
  const size = Number(input || 0);
  if (size < 1024) return `${size} B`;
  if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
  if (size < 1024 * 1024 * 1024) return `${(size / 1024 / 1024).toFixed(1)} MB`;
  return `${(size / 1024 / 1024 / 1024).toFixed(1)} GB`;
};

const validDay = (year: string, month: string, day: string) => {
  const yyyy = Number(year);
  const mm = Number(month);
  const dd = Number(day);
  const date = new Date(Date.UTC(yyyy, mm - 1, dd));
  if (
    yyyy < 2000 ||
    yyyy > 2100 ||
    date.getUTCFullYear() !== yyyy ||
    date.getUTCMonth() !== mm - 1 ||
    date.getUTCDate() !== dd
  ) {
    return undefined;
  }
  return `${year}-${month}-${day}`;
};

export const archiveDaysFromText = (value?: string | null) => {
  const text = fmt(value);
  const days = new Set<string>();
  for (const match of text.matchAll(/(?:^|\D)((?:20)\d{2})([01]\d)([0-3]\d)(?=\D|$)/g)) {
    const day = validDay(match[1], match[2], match[3]);
    if (day) days.add(day);
  }
  for (const match of text.matchAll(/(?:^|\D)((?:20)\d{2})[-_/]([01]\d)[-_/]([0-3]\d)(?=\D|$)/g)) {
    const day = validDay(match[1], match[2], match[3]);
    if (day) days.add(day);
  }
  return Array.from(days);
};

export const shortHash = (value?: string | null) => {
  const text = fmt(value);
  return text.length > 16 ? `${text.slice(0, 12)}...` : text;
};

export const statusLabel = (status?: ParseStatus) => {
  if (status === 'parsed') return '解析成功';
  if (status === 'partial') return '部分解析';
  return '解析失败';
};

export const deviceName = (source?: string | null, raw?: string | null) => {
  const value = fmt(source);
  if (value === '-') return '-';
  if (value === 'unknown://legacy') {
    const host = fmt(raw).match(/^\w{3}\s+\d+\s+\d{2}:\d{2}:\d{2}\s+(\S+)\s+/)?.[1];
    return host && host !== '-' ? `${host} / 历史导入` : '历史导入';
  }
  return value.replace(/^(udp|tcp|frozen):\/\//, '');
};

export const deviceProtocol = (source?: string | null) => {
  const value = fmt(source);
  if (value.startsWith('udp://')) return 'UDP';
  if (value.startsWith('tcp://')) return 'TCP';
  if (value.startsWith('frozen://')) return 'Frozen';
  return 'Unknown';
};

export const managedProtocol = (value?: string | null): 'UDP' | 'TCP' | 'TLS' => {
  const text = fmt(value).toUpperCase();
  if (text === 'TCP') return 'TCP';
  if (text === 'TLS') return 'TLS';
  return 'UDP';
};

export const deviceSourceKeys = (device: FirewallDevice) => {
  const keys = [device.host];
  if (device.port) keys.push(`${device.host}:${device.port}`);
  return Array.from(new Set(keys.filter(Boolean)));
};

export const isPublicIp = (value?: string | null) => {
  const ip = fmt(value);
  const parts = ip.split('.').map((part) => Number(part));
  if (parts.length !== 4 || parts.some((part) => !Number.isInteger(part) || part < 0 || part > 255)) {
    return null;
  }
  const [a, b] = parts;
  if (a === 10 || a === 127 || a === 0 || a >= 224) return null;
  if (a === 172 && b >= 16 && b <= 31) return null;
  if (a === 192 && b === 168) return null;
  if (a === 169 && b === 254) return null;
  if (a === 100 && b >= 64 && b <= 127) return null;
  return ip;
};

const isChinaRegion = (region?: string | null) => {
  if (!region) return true;
  const text = region.toLowerCase();
  return (
    region.includes('中国') ||
    text.includes('china') ||
    region.includes('内网') ||
    region.includes('局域网') ||
    region.includes('待识别') ||
    region.includes('未知')
  );
};

export const ipWithRegion = (value: string | null | undefined, region?: string | null) => {
  const ip = fmt(value);
  const foreign = Boolean(region && !isChinaRegion(region));
  return (
    <Space size={6}>
      <span className="mono">{ip}</span>
      {region ? (
        <Tag className={foreign ? 'ip-region-tag ip-region-tag-foreign' : 'ip-region-tag'}>
          {region}
        </Tag>
      ) : null}
    </Space>
  );
};

export const protocolTag = (value?: string | number | null) => {
  const text = fmt(value);
  if (text === '17') return <Tag color="blue">UDP</Tag>;
  if (text === '6') return <Tag color="geekblue">TCP</Tag>;
  return <Tag color="blue">{text}</Tag>;
};

export const admissionStateTag = (state?: string) => {
  if (state === 'trusted') return <Tag color="success">已信任</Tag>;
  if (state === 'blocked') return <Tag color="error">已阻断</Tag>;
  return <Tag color="processing">待审批</Tag>;
};

export const downloadUrl = (url: string, filename: string) => {
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  link.click();
};

export const localDay = (date: Date) => {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
};

export const eventTime = (value?: string | null) => fmt(value).replace('T', ' ').replace('Z', '').slice(0, 23);

export const parseRate = (total: number, parsed: number) =>
  total > 0 ? `${((parsed / total) * 100).toFixed(1)}%` : '-';

export const tableOptions = {
  density: true,
  fullScreen: true,
  setting: true,
  reload: false,
};

export type SearchResultRow = CanonicalEvent & {
  result_key: string;
  result_source: string;
  archive_path?: string | null;
  device_name?: string | null;
  geo_region?: string | null;
  src_geo_region?: string | null;
  dst_geo_region?: string | null;
};
