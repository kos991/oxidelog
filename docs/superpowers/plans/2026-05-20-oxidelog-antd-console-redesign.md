# OxideLog AntD Console Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redesign the OxideLog frontend as an Ant Design operations console with log search as the primary workflow, a NOC-style overview, and an adaptive parser operations workspace.

**Architecture:** Keep the existing `/oxidelog` route and `antd`/`@ant-design/pro-components` stack. Split the current large OxideLog page into focused components and page modules while preserving current service calls and backend APIs. Rewrite the log search workspace text and layout to remove mojibake and make hot/archive/all investigation the central workflow.

**Frame Direction:** Use the confirmed ESXi Builder-like console frame: a 72px collapsed icon rail, 56px top header, light gray workspace, white bordered panels, compact metric cards, and table-first work areas.

**Tech Stack:** React 19, TypeScript, Ant Design 6, Ant Design Pro Components 3, Jest, Testing Library, Umi Max, existing `src/services/oxidelog.ts`.

---

## Scope

Included:

- AntD console shell with default collapsed icon rail, header, and page content container.
- Optional expanded navigation state while keeping collapsed mode fully usable.
- NOC-style 运行态势 overview.
- Primary 日志检索 workspace with readable Chinese labels.
- Log search page owns export task creation and an in-page export task panel for refresh/cancel/retry/download history.
- Export download format rule: choose format at download time; short-range/small-result downloads offer CSV, ZST, or Parquet; long-range bulk downloads such as half-year or one-year log queries offer ZST or Parquet only.
- Parser management workspace for adaptive rules, diagnostics, profiles, and scopes.
- Source governance and archive pages preserved as management pages.
- Component split from the current monolithic `index.tsx`.
- Focused frontend tests for helpers and key rendered labels/counts.

Excluded:

- Backend API changes.
- Dark theme.
- Login/auth UI redesign.
- New UI libraries.

## File Structure

Create:

- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/types.ts`
- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/utils.tsx`
- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/components/Shell.tsx`
- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/components/MetricCard.tsx`
- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/components/StatusTags.tsx`
- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/pages/OverviewPage.tsx`
- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/pages/LogSearchPage.tsx`
- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/pages/ParserPage.tsx`
- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/pages/SourceGovernancePage.tsx`
- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/pages/AssetsPage.tsx`
- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/utils.test.ts`
- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/components/StatusTags.test.tsx`
- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/pages/ParserPage.test.tsx`
- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/pages/LogSearchPage.test.tsx`

Modify:

- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/index.tsx`
- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/LogSearchPanel.tsx`
- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/style.less`
- `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/services/oxidelog.ts` only if frontend types need partial-count fields already returned by API.

## Task 1: Extract Shared Types And Utilities

**Files:**

- Create: `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/types.ts`
- Create: `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/utils.tsx`
- Create: `ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/utils.test.ts`

- [ ] **Step 1: Write failing utility tests**

Add `utils.test.ts`:

```ts
import { archiveDaysFromText, bytes, fmt, statusLabel } from './utils';

describe('oxidelog utils', () => {
  it('formats empty values as dash', () => {
    expect(fmt(undefined)).toBe('-');
    expect(fmt(null)).toBe('-');
    expect(fmt('')).toBe('-');
    expect(fmt('tcp://127.0.0.1')).toBe('tcp://127.0.0.1');
  });

  it('formats byte sizes', () => {
    expect(bytes(12)).toBe('12 B');
    expect(bytes(2048)).toBe('2.0 KB');
    expect(bytes(5 * 1024 * 1024)).toBe('5.0 MB');
  });

  it('extracts archive days from common file names', () => {
    expect(archiveDaysFromText('events-20260520.parquet')).toEqual(['2026-05-20']);
    expect(archiveDaysFromText('frozen/2026-05-19/raw.zst')).toEqual(['2026-05-19']);
  });

  it('labels partial separately from failed', () => {
    expect(statusLabel('parsed')).toBe('解析成功');
    expect(statusLabel('partial')).toBe('部分解析');
    expect(statusLabel('failed')).toBe('解析失败');
  });
});
```

- [ ] **Step 2: Run the test and verify it fails**

Run:

```powershell
cd ant-design-pro-6.0.1/ant-design-pro-6.0.1
npm test -- src/pages/oxidelog/utils.test.ts
```

Expected: fail because `utils.tsx` does not exist.

- [ ] **Step 3: Create shared type definitions**

Add `types.ts`:

```ts
import type {
  ArchiveFile,
  ArchiveIndexRow,
  CanonicalEvent,
  FirewallDevice,
} from '@/services/oxidelog';

export type PageKey = 'overview' | 'logs' | 'devices' | 'admission' | 'parser' | 'assets';

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
  partial: number;
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

export type SearchShortcut = {
  label: string;
  filters: Record<string, string>;
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
```

- [ ] **Step 4: Create utilities**

Add `utils.tsx`:

```tsx
import React from 'react';
import { Space, Tag } from 'antd';
import type { CanonicalEvent, FirewallDevice } from '@/services/oxidelog';

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

export const statusLabel = (status?: CanonicalEvent['parse_status']) => {
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

export const deviceSourceKeys = (device: FirewallDevice) => {
  const keys = [device.host];
  if (device.port) keys.push(`${device.host}:${device.port}`);
  return Array.from(new Set(keys.filter(Boolean)));
};

export const ipWithRegion = (value: string | null | undefined, region?: string | null) => {
  const ip = fmt(value);
  return (
    <Space size={6}>
      {region ? <Tag className="ip-region-tag">{region}</Tag> : null}
      <span className="mono">{ip}</span>
    </Space>
  );
};

export const localDay = (date: Date) => {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
};

export const tableOptions = {
  density: true,
  fullScreen: true,
  setting: true,
  reload: false,
};
```

- [ ] **Step 5: Run utility tests**

Run:

```powershell
cd ant-design-pro-6.0.1/ant-design-pro-6.0.1
npm test -- src/pages/oxidelog/utils.test.ts
```

Expected: pass.

## Task 2: Shared Status And Metric Components

**Files:**

- Create: `src/pages/oxidelog/components/StatusTags.tsx`
- Create: `src/pages/oxidelog/components/MetricCard.tsx`
- Create: `src/pages/oxidelog/components/StatusTags.test.tsx`

- [ ] **Step 1: Write failing status tag tests**

Add `components/StatusTags.test.tsx`:

```tsx
import React from 'react';
import { render, screen } from '@testing-library/react';
import { ParseStatusTag, AdaptiveRuleStatusTag } from './StatusTags';

describe('StatusTags', () => {
  it('renders parse statuses with readable Chinese labels', () => {
    render(
      <>
        <ParseStatusTag status="parsed" />
        <ParseStatusTag status="partial" />
        <ParseStatusTag status="failed" />
      </>,
    );

    expect(screen.getByText('解析成功')).toBeInTheDocument();
    expect(screen.getByText('部分解析')).toBeInTheDocument();
    expect(screen.getByText('解析失败')).toBeInTheDocument();
  });

  it('renders adaptive rule lifecycle statuses', () => {
    render(
      <>
        <AdaptiveRuleStatusTag status="active" />
        <AdaptiveRuleStatusTag status="shadow" />
        <AdaptiveRuleStatusTag status="disabled" />
      </>,
    );

    expect(screen.getByText('Active')).toBeInTheDocument();
    expect(screen.getByText('Shadow')).toBeInTheDocument();
    expect(screen.getByText('Disabled')).toBeInTheDocument();
  });
});
```

- [ ] **Step 2: Run the test and verify it fails**

Run:

```powershell
cd ant-design-pro-6.0.1/ant-design-pro-6.0.1
npm test -- src/pages/oxidelog/components/StatusTags.test.tsx
```

Expected: fail because `StatusTags.tsx` does not exist.

- [ ] **Step 3: Implement status tags**

Add `components/StatusTags.tsx`:

```tsx
import React from 'react';
import { Tag } from 'antd';
import type { AdaptiveRuleStatus, CanonicalEvent } from '@/services/oxidelog';

export const ParseStatusTag: React.FC<{ status?: CanonicalEvent['parse_status'] }> = ({ status }) => {
  if (status === 'parsed') return <Tag color="success">解析成功</Tag>;
  if (status === 'partial') return <Tag color="warning">部分解析</Tag>;
  return <Tag color="error">解析失败</Tag>;
};

export const AdmissionStateTag: React.FC<{ state?: string }> = ({ state }) => {
  if (state === 'trusted') return <Tag color="success">已信任</Tag>;
  if (state === 'blocked') return <Tag color="error">已阻断</Tag>;
  return <Tag color="processing">待审批</Tag>;
};

export const ProtocolTag: React.FC<{ value?: string | number | null }> = ({ value }) => {
  const text = value === undefined || value === null || value === '' ? '-' : String(value);
  if (text === '17') return <Tag color="blue">UDP</Tag>;
  if (text === '6') return <Tag color="geekblue">TCP</Tag>;
  return <Tag color="blue">{text}</Tag>;
};

export const AdaptiveRuleStatusTag: React.FC<{ status: AdaptiveRuleStatus | string }> = ({ status }) => {
  const labels: Record<string, string> = {
    active: 'Active',
    shadow: 'Shadow',
    shadow_recovering: 'Recovering',
    disabled: 'Disabled',
  };
  const colors: Record<string, string> = {
    active: 'success',
    shadow: 'processing',
    shadow_recovering: 'warning',
    disabled: 'error',
  };
  return <Tag color={colors[status] || 'default'}>{labels[status] || status}</Tag>;
};
```

- [ ] **Step 4: Implement metric card**

Add `components/MetricCard.tsx`:

```tsx
import React from 'react';
import { ProCard } from '@ant-design/pro-components';
import { Typography } from 'antd';

const { Text } = Typography;

export type MetricCardProps = {
  title: string;
  value: React.ReactNode;
  icon?: React.ReactNode;
  tone?: 'default' | 'success' | 'warning' | 'danger';
  extra?: React.ReactNode;
};

export const MetricCard: React.FC<MetricCardProps> = ({ title, value, icon, tone = 'default', extra }) => (
  <ProCard className={`metric-card metric-card-${tone}`} variant="outlined">
    <div className="metric-card-main">
      <div>
        <Text type="secondary">{title}</Text>
        <strong>{value}</strong>
        {extra ? <div className="metric-card-extra">{extra}</div> : null}
      </div>
      {icon ? <div className="metric-card-icon">{icon}</div> : null}
    </div>
  </ProCard>
);
```

- [ ] **Step 5: Run component tests**

Run:

```powershell
cd ant-design-pro-6.0.1/ant-design-pro-6.0.1
npm test -- src/pages/oxidelog/components/StatusTags.test.tsx
```

Expected: pass.

## Task 3: Build The AntD Shell

**Files:**

- Create: `src/pages/oxidelog/components/Shell.tsx`
- Modify: `src/pages/oxidelog/style.less`

- [ ] **Step 0: Match the approved reference frame**

Implement the shell around the Pencil preview `05 参考框架 / 收缩侧栏`: 72px default icon rail, OxideLog mark-only branding in the rail, navigation icons with tooltips/accessible labels, bottom health dot, 56px top header with product name/subtitle, and a gray workspace containing white bordered panels.

- [ ] **Step 1: Implement shell component**

Add `components/Shell.tsx`:

```tsx
import {
  CloudServerOutlined,
  DashboardOutlined,
  DatabaseOutlined,
  FileSearchOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  ReloadOutlined,
  SafetyCertificateOutlined,
  SearchOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons';
import { Button, Layout, Menu, Space, Typography } from 'antd';
import React from 'react';
import type { PageKey } from '../types';

const { Header, Sider, Content } = Layout;
const { Text, Title } = Typography;

const navItems = [
  { key: 'overview', icon: <DashboardOutlined />, label: '运行态势' },
  { key: 'logs', icon: <SearchOutlined />, label: '日志检索' },
  { key: 'devices', icon: <CloudServerOutlined />, label: '设备来源' },
  { key: 'admission', icon: <SafetyCertificateOutlined />, label: '准入控制' },
  { key: 'parser', icon: <ThunderboltOutlined />, label: '解析器管理' },
  { key: 'assets', icon: <DatabaseOutlined />, label: '归档资产' },
] satisfies { key: PageKey; icon: React.ReactNode; label: string }[];

const pageCopy: Record<PageKey, { title: string; subtitle: string }> = {
  overview: { title: '运行态势', subtitle: '实时趋势、异常、设备健康与系统状态' },
  logs: { title: '日志检索', subtitle: '热库、冷库与归档日志的一站式调查工作台' },
  devices: { title: '设备来源', subtitle: '纳管设备、观测来源与设备日志健康' },
  admission: { title: '准入控制', subtitle: '未知来源审批、阻断与信任配置' },
  parser: { title: '解析器管理', subtitle: '自学习规则、诊断、Profile 与 Scope 状态' },
  assets: { title: '归档资产', subtitle: 'Parquet 冷库、Frozen 原始归档与索引状态' },
};

export type OxideLogShellProps = {
  activePage: PageKey;
  collapsed: boolean;
  loading: boolean;
  workerErrors?: number;
  onChangePage: (page: PageKey) => void;
  onToggleCollapsed: () => void;
  onRefresh: () => void;
  children: React.ReactNode;
};

export const OxideLogShell: React.FC<OxideLogShellProps> = ({
  activePage,
  collapsed,
  loading,
  workerErrors,
  onChangePage,
  onToggleCollapsed,
  onRefresh,
  children,
}) => {
  const copy = pageCopy[activePage];
  return (
    <Layout className="oxidelog-shell">
      <Sider className="app-sider" width={232} breakpoint="lg" collapsed={collapsed} collapsedWidth={64} trigger={null}>
        <div className="brand">
          <div className="brand-mark">OL</div>
          <div className="brand-text">
            <strong>OxideLog</strong>
            <span>NAT 日志中心</span>
          </div>
        </div>
        <Menu
          mode="inline"
          selectedKeys={[activePage]}
          onClick={({ key }) => onChangePage(key as PageKey)}
          items={navItems}
        />
        <div className="sider-health">
          <FileSearchOutlined />
          <span>{workerErrors && workerErrors > 0 ? `Worker 错误 ${workerErrors}` : '运行正常'}</span>
        </div>
        <div className="sider-collapse-control">
          <Button
            type="text"
            aria-label={collapsed ? '展开左侧导航' : '收起左侧导航'}
            icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
            onClick={onToggleCollapsed}
          />
        </div>
      </Sider>
      <Layout>
        <Header className="app-header">
          <div className="header-left">
            <div>
              <Title level={4}>{copy.title}</Title>
              <Text type="secondary">{copy.subtitle}</Text>
            </div>
          </div>
          <Space size={8}>
            <Button icon={<ReloadOutlined />} loading={loading} onClick={onRefresh}>
              刷新
            </Button>
          </Space>
        </Header>
        <Content className="app-content">{children}</Content>
      </Layout>
    </Layout>
  );
};
```

- [ ] **Step 2: Update shell styles**

In `style.less`, keep the existing `.oxidelog-shell` block but update or add these rules:

```less
.oxidelog-shell {
  min-height: 100vh;
  background: #f5f7fb;
  color: #172033;

  .app-content {
    min-width: 0;
    min-height: calc(100vh - 64px);
    padding: 16px;
    overflow: auto;
  }

  .page-stack {
    width: 100%;
  }

  .sider-health {
    display: flex;
    min-height: 38px;
    align-items: center;
    gap: 8px;
    margin: 8px;
    padding: 8px 10px;
    border: 1px solid #e5edf7;
    border-radius: 6px;
    background: #f8fbff;
    color: #475569;
    font-size: 12px;
  }

  .metric-card-main {
    display: flex;
    min-height: 88px;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
  }

  .metric-card-main strong {
    display: block;
    margin-top: 4px;
    color: #172033;
    font-size: 28px;
    line-height: 34px;
  }

  .metric-card-icon {
    display: grid;
    width: 42px;
    height: 42px;
    flex: 0 0 auto;
    place-items: center;
    border-radius: 8px;
    background: #edf4ff;
    color: #1677ff;
    font-size: 20px;
  }
}
```

- [ ] **Step 3: Run TypeScript**

Run:

```powershell
cd ant-design-pro-6.0.1/ant-design-pro-6.0.1
npm run tsc
```

Expected: pass or fail only because pages have not been wired yet. If it fails on `Shell.tsx`, fix the component.

## Task 4: Implement OverviewPage

**Files:**

- Create: `src/pages/oxidelog/pages/OverviewPage.tsx`

- [ ] **Step 1: Implement overview page**

Add `OverviewPage.tsx`:

```tsx
import {
  AlertOutlined,
  CheckCircleOutlined,
  CloudServerOutlined,
  DashboardOutlined,
  SearchOutlined,
} from '@ant-design/icons';
import { Button, Col, Row, Space, Table, Tag, Typography } from 'antd';
import React, { useMemo } from 'react';
import type {
  AdmissionCase,
  CanonicalEvent,
  MinuteMetricPoint,
  ParserDiagnostic,
  ParserScopeState,
  SourceMetricPoint,
} from '@/services/oxidelog';
import { MetricCard } from '../components/MetricCard';
import { ParseStatusTag } from '../components/StatusTags';
import type { PageKey, RecentStreamRow } from '../types';
import { fmt, statusLabel } from '../utils';

const { Text } = Typography;

export type OverviewPageProps = {
  events: CanonicalEvent[];
  minuteMetrics: MinuteMetricPoint[];
  sourceMetrics: SourceMetricPoint[];
  parserDiagnostics: ParserDiagnostic[];
  parserScopes: ParserScopeState[];
  admissionCases: AdmissionCase[];
  displayDeviceName: (source?: string | null, raw?: string | null) => string;
  onNavigate: (page: PageKey, filters?: Record<string, string>) => void;
};

export const OverviewPage: React.FC<OverviewPageProps> = ({
  events,
  minuteMetrics,
  sourceMetrics,
  parserDiagnostics,
  parserScopes,
  admissionCases,
  displayDeviceName,
  onNavigate,
}) => {
  const total = minuteMetrics.reduce((sum, point) => sum + point.total, 0);
  const parsed = minuteMetrics.reduce((sum, point) => sum + point.parsed, 0);
  const failed = minuteMetrics.reduce((sum, point) => sum + point.failed, 0);
  const partial = minuteMetrics.reduce((sum, point) => sum + ((point as any).partial || 0), 0);
  const parseRate = total > 0 ? `${((parsed / total) * 100).toFixed(1)}%` : '-';
  const pendingAdmission = admissionCases.filter((row) => row.state === 'pending').length;
  const quarantined = parserScopes.filter(
    (scope) => scope.adaptive_quarantine_until && new Date(scope.adaptive_quarantine_until) > new Date(),
  ).length;
  const trendMax = Math.max(...minuteMetrics.map((point) => point.total), 1);
  const trendBars = minuteMetrics.slice(-96);

  const streamRows = useMemo<RecentStreamRow[]>(
    () =>
      events.slice(0, 30).map((event) => ({
        key: event.event_id,
        time: fmt(event.ingest_time).replace('T', ' ').replace('Z', '').slice(0, 23),
        status: event.parse_status,
        statusLabel: statusLabel(event.parse_status),
        device: displayDeviceName(event.source_addr, event.raw),
        raw: fmt(event.raw),
      })),
    [events, displayDeviceName],
  );

  const unhealthySources = sourceMetrics
    .filter((source) => source.failed > 0 || ((source as any).partial || 0) > 0)
    .slice(0, 6);

  return (
    <Space direction="vertical" size={16} className="page-stack">
      <Row gutter={[16, 16]}>
        <Col xs={24} md={12} xl={6}>
          <MetricCard title="近 24 小时日志" value={total.toLocaleString()} icon={<DashboardOutlined />} />
        </Col>
        <Col xs={24} md={12} xl={6}>
          <MetricCard title="解析成功率" value={parseRate} tone="success" icon={<CheckCircleOutlined />} />
        </Col>
        <Col xs={24} md={12} xl={6}>
          <MetricCard title="部分/失败" value={`${partial}/${failed}`} tone={failed > 0 ? 'warning' : 'default'} icon={<AlertOutlined />} />
        </Col>
        <Col xs={24} md={12} xl={6}>
          <MetricCard title="待准入 / Quarantine" value={`${pendingAdmission}/${quarantined}`} tone={pendingAdmission || quarantined ? 'warning' : 'default'} icon={<CloudServerOutlined />} />
        </Col>
      </Row>

      <Row gutter={[16, 16]} className="overview-grid">
        <Col xs={24} xl={15}>
          <div className="panel">
            <div className="panel-header">
              <div>
                <strong>24 小时日志趋势</strong>
                <Text type="secondary">按分钟聚合，显示最近 96 个点</Text>
              </div>
              <Button icon={<SearchOutlined />} onClick={() => onNavigate('logs')}>
                进入检索
              </Button>
            </div>
            <div className="trend-chart">
              {trendBars.map((point) => (
                <span key={point.bucket_minute} style={{ height: `${Math.max(4, (point.total / trendMax) * 100)}%` }} />
              ))}
            </div>
          </div>
        </Col>
        <Col xs={24} xl={9}>
          <div className="panel">
            <div className="panel-header">
              <strong>需要关注</strong>
              <Text type="secondary">异常、待准入与解析风险</Text>
            </div>
            <Space direction="vertical" size={8} style={{ width: '100%' }}>
              <Button block onClick={() => onNavigate('logs', { include_failed: 'true' })}>
                查看失败和部分解析日志
              </Button>
              <Button block onClick={() => onNavigate('admission')}>
                处理待准入来源
              </Button>
              <Button block onClick={() => onNavigate('parser')}>
                查看解析器诊断
              </Button>
            </Space>
          </div>
        </Col>
      </Row>

      <Row gutter={[16, 16]}>
        <Col xs={24} xl={14}>
          <div className="panel">
            <div className="panel-header">
              <strong>最近日志</strong>
              <Text type="secondary">用于判断接入是否持续流入</Text>
            </div>
            <Table
              size="small"
              rowKey="key"
              pagination={false}
              dataSource={streamRows}
              columns={[
                { title: '时间', dataIndex: 'time', width: 190, render: (value) => <span className="mono">{value}</span> },
                { title: '状态', dataIndex: 'status', width: 110, render: (value) => <ParseStatusTag status={value} /> },
                { title: '设备', dataIndex: 'device', width: 180, ellipsis: true },
                { title: '原始日志', dataIndex: 'raw', ellipsis: true, render: (value) => <span className="mono raw-log">{value}</span> },
              ]}
            />
          </div>
        </Col>
        <Col xs={24} xl={10}>
          <div className="panel">
            <div className="panel-header">
              <strong>异常来源</strong>
              <Text type="secondary">近 24 小时有失败或部分解析</Text>
            </div>
            <Table
              size="small"
              rowKey="source_addr"
              pagination={false}
              dataSource={unhealthySources}
              columns={[
                { title: '来源', dataIndex: 'source_addr', ellipsis: true, render: (value) => <span className="mono">{value}</span> },
                { title: '总数', dataIndex: 'total', width: 80 },
                { title: '失败', dataIndex: 'failed', width: 80, render: (value) => <Tag color={value ? 'error' : 'default'}>{value}</Tag> },
              ]}
            />
          </div>
        </Col>
      </Row>
    </Space>
  );
};
```

- [ ] **Step 2: Add panel styles**

In `style.less`, add:

```less
.oxidelog-shell {
  .panel {
    border: 1px solid #d9e2ef;
    border-radius: 8px;
    background: #ffffff;
    padding: 16px;
  }

  .panel-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 12px;

    strong {
      display: block;
      color: #172033;
      font-size: 14px;
      font-weight: 650;
    }

    .ant-typography-secondary {
      display: block;
      margin-top: 2px;
      font-size: 12px;
    }
  }
}
```

- [ ] **Step 3: Run TypeScript**

Run:

```powershell
cd ant-design-pro-6.0.1/ant-design-pro-6.0.1
npm run tsc
```

Expected: pass after imports are wired in later tasks, or only fail because `OverviewPage` is not yet used.

## Task 5: Rewrite LogSearchPage

**Files:**

- Create: `src/pages/oxidelog/pages/LogSearchPage.tsx`
- Modify: `src/pages/oxidelog/LogSearchPanel.tsx`
- Create: `src/pages/oxidelog/pages/LogSearchPage.test.tsx`

- [ ] **Step 1: Write failing readable-label test**

Add `pages/LogSearchPage.test.tsx`:

```tsx
import React from 'react';
import { render, screen } from '@testing-library/react';
import { LogSearchPage } from './LogSearchPage';

describe('LogSearchPage', () => {
  it('renders readable Chinese investigation actions', () => {
    render(
      <LogSearchPage
        events={[]}
        loading={false}
        archiveFiles={[]}
        frozenFiles={[]}
        deviceFilterOptions={[]}
        hotLogDaySet={new Set()}
        coldLogDaySet={new Set()}
        displayDeviceName={() => '-'}
      />,
    );

    expect(screen.getByText('日志检索')).toBeInTheDocument();
    expect(screen.getByText('查询热库')).toBeInTheDocument();
    expect(screen.getByText('追溯归档')).toBeInTheDocument();
    expect(screen.getByText('全量检索')).toBeInTheDocument();
    expect(screen.getByText('创建导出')).toBeInTheDocument();
  });
});
```

- [ ] **Step 2: Run the test and verify it fails**

Run:

```powershell
cd ant-design-pro-6.0.1/ant-design-pro-6.0.1
npm test -- src/pages/oxidelog/pages/LogSearchPage.test.tsx
```

Expected: fail because `LogSearchPage.tsx` does not exist.

- [ ] **Step 3: Implement LogSearchPage**

Move the behavior from `LogSearchPanel.tsx` into `pages/LogSearchPage.tsx`, but replace all mojibake strings with readable Chinese:

- `日志检索`
- `更多条件`
- `日期范围`
- `源 IP`
- `目的 IP`
- `设备来源`
- `协议`
- `动作`
- `关键字`
- `返回上限`
- `解析失败`
- `包含`
- `排除失败`
- `查询热库`
- `追溯归档`
- `全量检索`
- `重置`
- `检索结果`
- `创建导出`
- `去导出页`
- `下载时选择格式`

Keep these existing functions and flows:

- `normalizeSearchValues`
- `searchUnified`
- `searchExportUrl`
- `createExportJob`
- `fetchExportJob`
- `fetchIpRegion`
- hot/archive/all search modes
- calendar hot/cold markers
- `initialFilters`

Use a left filter rail on desktop:

```tsx
return (
  <div className="log-workbench">
    <aside className="log-filter-rail">
      {/* ProForm vertical filters */}
    </aside>
    <section className="log-results-pane">
      {/* summary strip, action buttons, ProTable */}
    </section>
  </div>
);
```

- [ ] **Step 4: Make LogSearchPanel a compatibility wrapper**

Replace `LogSearchPanel.tsx` content with:

```tsx
export { LogSearchPage as default } from './pages/LogSearchPage';
export type { LogSearchPanelProps } from './pages/LogSearchPage';
```

This keeps the existing import in `index.tsx` working until `index.tsx` is refactored.

- [ ] **Step 5: Add workbench styles**

In `style.less`, add:

```less
.oxidelog-shell {
  .log-workbench {
    display: grid;
    grid-template-columns: 300px minmax(0, 1fr);
    gap: 16px;
    align-items: start;
  }

  .log-filter-rail,
  .log-results-pane {
    border: 1px solid #d9e2ef;
    border-radius: 8px;
    background: #ffffff;
    padding: 16px;
  }

  .log-filter-rail {
    position: sticky;
    top: 16px;
  }

  .log-action-bar {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-top: 12px;
  }
}

@media (max-width: 992px) {
  .oxidelog-shell {
    .log-workbench {
      grid-template-columns: 1fr;
    }

    .log-filter-rail {
      position: static;
    }
  }
}
```

- [ ] **Step 6: Run LogSearchPage tests**

Run:

```powershell
cd ant-design-pro-6.0.1/ant-design-pro-6.0.1
npm test -- src/pages/oxidelog/pages/LogSearchPage.test.tsx
```

Expected: pass.

## Task 6: Implement ParserPage

**Files:**

- Create: `src/pages/oxidelog/pages/ParserPage.tsx`
- Create: `src/pages/oxidelog/pages/ParserPage.test.tsx`

- [ ] **Step 1: Write failing parser page test**

Add `ParserPage.test.tsx`:

```tsx
import React from 'react';
import { render, screen } from '@testing-library/react';
import { ParserPage } from './ParserPage';

describe('ParserPage', () => {
  it('renders adaptive rule lifecycle counts', () => {
    render(
      <ParserPage
        loading={false}
        adaptiveRules={[
          { rule_id: 'active-rule', scope_key: 'source:tcp://1.1.1.1', raw_key: 'dst', canonical_field: 'dst_ip', value_type: 'ip', status: 'active', confidence: 1, wins: 10, sample_count: 10, created_at: '2026-05-20T00:00:00Z' },
          { rule_id: 'shadow-rule', scope_key: 'source:tcp://1.1.1.1', raw_key: 'act', canonical_field: 'action', value_type: 'action', status: 'shadow', confidence: 0.6, wins: 6, sample_count: 10, created_at: '2026-05-20T00:00:00Z' },
          { rule_id: 'disabled-rule', scope_key: 'source:tcp://1.1.1.1', raw_key: 'bad', canonical_field: 'dst_ip', value_type: 'ip', status: 'disabled', confidence: 0.2, wins: 2, sample_count: 10, created_at: '2026-05-20T00:00:00Z' },
        ]}
        parserDiagnostics={[]}
        parserProfiles={[]}
        parserScopes={[]}
        onEnableRule={jest.fn()}
        onDisableRule={jest.fn()}
      />,
    );

    expect(screen.getByText('活跃规则')).toBeInTheDocument();
    expect(screen.getByText('Shadow 规则')).toBeInTheDocument();
    expect(screen.getByText('禁用规则')).toBeInTheDocument();
    expect(screen.getAllByText('1').length).toBeGreaterThanOrEqual(3);
  });
});
```

- [ ] **Step 2: Run the test and verify it fails**

Run:

```powershell
cd ant-design-pro-6.0.1/ant-design-pro-6.0.1
npm test -- src/pages/oxidelog/pages/ParserPage.test.tsx
```

Expected: fail because `ParserPage.tsx` does not exist.

- [ ] **Step 3: Implement ParserPage**

Add a page with:

- metric strip for active/shadow/disabled/quarantine.
- tabs: `规则`, `诊断`, `Profiles`, `Scopes`.
- adaptive rule table with enable/disable buttons delegated through props.

Props:

```ts
export type ParserPageProps = {
  loading: boolean;
  adaptiveRules: AdaptiveRule[];
  parserDiagnostics: ParserDiagnostic[];
  parserProfiles: ParserProfile[];
  parserScopes: ParserScopeState[];
  onEnableRule: (ruleId: string) => Promise<void>;
  onDisableRule: (ruleId: string) => Promise<void>;
};
```

Use `AdaptiveRuleStatusTag` from `components/StatusTags.tsx`.

- [ ] **Step 4: Run parser page test**

Run:

```powershell
cd ant-design-pro-6.0.1/ant-design-pro-6.0.1
npm test -- src/pages/oxidelog/pages/ParserPage.test.tsx
```

Expected: pass.

## Task 7: Split Devices, Admission, And Assets Pages

**Files:**

- Create: `src/pages/oxidelog/pages/AssetsPage.tsx`
- Modify: `src/pages/oxidelog/index.tsx`

- [ ] **Step 1: Extract AssetsPage**

Move archive assets and frozen archive index tables from `index.tsx` into `AssetsPage.tsx`.

Props:

```ts
export type AssetsPageProps = {
  loading: boolean;
  assetRows: AssetRow[];
  archiveIndexRows: ArchiveIndexTableRow[];
  onRebuildArchiveIndex: () => Promise<void>;
};
```

- [ ] **Step 2: Run TypeScript**

Run:

```powershell
cd ant-design-pro-6.0.1/ant-design-pro-6.0.1
npm run tsc
```

Expected: fail only where `index.tsx` still imports old local render functions. Fix imports as pages are wired.

## Task 8: Rewrite index.tsx As Orchestrator

**Files:**

- Modify: `src/pages/oxidelog/index.tsx`

- [ ] **Step 1: Replace inline shell with OxideLogShell**

In `index.tsx`, import:

```tsx
import { OxideLogShell } from './components/Shell';
import { OverviewPage } from './pages/OverviewPage';
import LogSearchPanel from './LogSearchPanel';
import { SourceGovernancePage } from './pages/SourceGovernancePage';
import { ParserPage } from './pages/ParserPage';
import { AssetsPage } from './pages/AssetsPage';
import type { PageKey } from './types';
```

Return:

```tsx
return (
  <OxideLogShell
    activePage={activePage}
    collapsed={collapsed}
    loading={loading}
    workerErrors={systemStatus?.metrics?.worker_errors}
    onChangePage={setActivePage}
    onToggleCollapsed={() => setCollapsed((value) => !value)}
    onRefresh={load}
  >
    {renderPage()}
    {deviceModal}
    {ipRegionModal}
  </OxideLogShell>
);
```

If `systemStatus` is not currently loaded, either add `fetchStatus()` to `load()` or pass `undefined`.

- [ ] **Step 2: Implement navigation with filters**

Add:

```ts
const navigateToLogs = (filters?: Record<string, string>) => {
  setLogFilters(filters || {});
  setActivePage('logs');
};
```

Pass this into `OverviewPage`, `SourceGovernancePage`, and `AssetsPage` where relevant.

- [ ] **Step 3: Replace renderPage implementation**

Use:

```tsx
const renderPage = () => {
  if (activePage === 'overview') {
    return (
      <OverviewPage
        events={events}
        minuteMetrics={minuteMetrics}
        sourceMetrics={sourceMetrics}
        parserDiagnostics={parserDiagnostics}
        parserScopes={parserScopes}
        admissionCases={admissionCaseRows}
        displayDeviceName={displayDeviceName}
        onNavigate={navigateToLogsOrPage}
      />
    );
  }
  if (activePage === 'logs') {
    return (
      <LogSearchPanel
        events={events}
        loading={loading}
        archiveFiles={archiveFiles}
        frozenFiles={frozenFiles}
        deviceFilterOptions={deviceFilterOptions}
        hotLogDaySet={hotLogDaySet}
        coldLogDaySet={coldLogDaySet}
        displayDeviceName={displayDeviceName}
        initialFilters={logFilters}
      />
    );
  }
  if (activePage === 'devices') return <SourceGovernancePage {...sourceGovernancePageProps} />;
  if (activePage === 'admission') return <SourceGovernancePage {...sourceGovernancePageProps} />;
  if (activePage === 'parser') return <ParserPage {...parserPageProps} />;
  return <AssetsPage {...assetsPageProps} />;
};
```

- [ ] **Step 4: Remove obsolete inline render functions**

Delete these from `index.tsx` after their pages are wired:

- `renderOverview`
- `renderSourceGovernance`
- `renderAssets`
- `renderParser`

Keep data loading and modal submit handlers in `index.tsx` for this iteration.

- [ ] **Step 5: Run TypeScript**

Run:

```powershell
cd ant-design-pro-6.0.1/ant-design-pro-6.0.1
npm run tsc
```

Expected: pass.

## Task 9: Final Styling Pass

**Files:**

- Modify: `src/pages/oxidelog/style.less`

- [ ] **Step 1: Remove nested-card styling pressure**

Ensure pages use `.panel` and `.metric-card` rather than placing `ProCard` inside `ProCard` for whole sections.

- [ ] **Step 2: Ensure responsive behavior**

Add or verify:

```less
@media (max-width: 992px) {
  .oxidelog-shell {
    .app-header {
      height: auto;
      min-height: 64px;
      align-items: flex-start;
      flex-direction: column;
      gap: 8px;
      padding-block: 10px;
    }

    .panel-header {
      align-items: flex-start;
      flex-direction: column;
    }
  }
}
```

- [ ] **Step 3: Run visual smoke locally**

Run:

```powershell
cd ant-design-pro-6.0.1/ant-design-pro-6.0.1
npm run tsc
npm run build
```

Expected: TypeScript and build pass.

## Task 10: Verification

**Files:**

- No new files unless tests reveal small fixes.

- [ ] **Step 1: Run OxideLog frontend tests**

Run:

```powershell
cd ant-design-pro-6.0.1/ant-design-pro-6.0.1
npm test -- src/pages/oxidelog
```

Expected: all OxideLog frontend tests pass.

- [ ] **Step 2: Run TypeScript**

Run:

```powershell
cd ant-design-pro-6.0.1/ant-design-pro-6.0.1
npm run tsc
```

Expected: pass.

- [ ] **Step 3: Build frontend**

Run:

```powershell
cd ant-design-pro-6.0.1/ant-design-pro-6.0.1
npm run build
```

Expected: pass and generate `dist` assets.

- [ ] **Step 4: Backend static smoke**

If frontend assets are copied to `web/`, run:

```powershell
$env:RUSTFLAGS='-l Rstrtmgr'
& "$env:USERPROFILE\.cargo\bin\cargo.exe" test -p fwlog-api root_serves_embedded_chinese_ui -- --test-threads=1
```

Expected: pass if `web/index.html` references `/umi.` or update the test to match the current Umi output only after confirming static serving works.

## Self-Review

Spec coverage:

- B primary log search is implemented in Tasks 5 and 8.
- A NOC-style overview is implemented in Task 4.
- C parser operations workspace is implemented in Task 6.
- AntD shell and component split are implemented in Tasks 2, 3, 7, and 8.
- Tests and verification are covered in Tasks 1, 2, 5, 6, and 10.

No intentional backend API changes are included.
