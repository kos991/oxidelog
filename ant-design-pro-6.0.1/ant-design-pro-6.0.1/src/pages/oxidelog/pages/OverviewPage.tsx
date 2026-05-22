import {
  AlertOutlined,
  CloudServerOutlined,
  DatabaseOutlined,
  FileSearchOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons';
import { ProTable } from '@ant-design/pro-components';
import type { ProColumns } from '@ant-design/pro-components';
import { Col, Row, Space, Tag, Typography } from 'antd';
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
import type { DisplayDeviceName, PageKey, RecentStreamRow } from '../types';
import { eventTime, fmt, parseRate, statusLabel } from '../utils';

const { Text } = Typography;

export type OverviewPageProps = {
  events: CanonicalEvent[];
  minuteMetrics: MinuteMetricPoint[];
  sourceMetrics: SourceMetricPoint[];
  parserDiagnostics: ParserDiagnostic[];
  parserScopes: ParserScopeState[];
  admissionCases: AdmissionCase[];
  displayDeviceName: DisplayDeviceName;
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
  const pendingAdmission = admissionCases.filter((row) => row.state === 'pending').length;
  const quarantined = parserScopes.filter(
    (scope) => scope.adaptive_quarantine_until && new Date(scope.adaptive_quarantine_until) > new Date(),
  ).length;
  const trendMax = Math.max(...minuteMetrics.map((point) => point.total), 1);
  const trendBars = minuteMetrics.slice(-96);

  const streamRows = useMemo<RecentStreamRow[]>(
    () =>
      events.slice(0, 40).map((event) => ({
        key: event.event_id,
        time: eventTime(event.ingest_time),
        status: event.parse_status,
        statusLabel: statusLabel(event.parse_status),
        device: displayDeviceName(event.source_addr, event.raw),
        raw: fmt(event.raw),
      })),
    [events, displayDeviceName],
  );

  const sourceRows = sourceMetrics
    .filter((source) => source.failed > 0)
    .slice(0, 8);

  const streamColumns: ProColumns<RecentStreamRow>[] = [
    { title: '接收时间', dataIndex: 'time', width: 190, render: (_, row) => <span className="mono">{row.time}</span> },
    { title: '状态', dataIndex: 'status', width: 110, render: (_, row) => <ParseStatusTag status={row.status} /> },
    { title: '设备', dataIndex: 'device', width: 180, ellipsis: true },
    { title: '原始日志', dataIndex: 'raw', ellipsis: true, render: (_, row) => <span className="mono raw-log">{row.raw}</span> },
  ];

  return (
    <Space direction="vertical" size={16} className="page-stack">
      <Row gutter={[16, 16]}>
        <Col xs={24} md={12} xl={6}>
          <MetricCard title="近24小时日志" value={total.toLocaleString()} icon={<FileSearchOutlined />} />
        </Col>
        <Col xs={24} md={12} xl={6}>
          <MetricCard title="解析成功率" value={parseRate(total, parsed)} tone="success" icon={<ThunderboltOutlined />} />
        </Col>
        <Col xs={24} md={12} xl={6}>
          <MetricCard title="解析失败" value={failed.toLocaleString()} tone={failed > 0 ? 'warning' : 'default'} icon={<AlertOutlined />} />
        </Col>
        <Col xs={24} md={12} xl={6}>
          <MetricCard title="待准入 / 隔离 Scope" value={`${pendingAdmission}/${quarantined}`} tone={pendingAdmission || quarantined ? 'warning' : 'default'} icon={<CloudServerOutlined />} />
        </Col>
      </Row>

      <Row gutter={[16, 16]}>
        <Col xs={24} xl={15}>
          <div className="panel">
            <div className="panel-header">
              <div>
                <strong>接入吞吐趋势</strong>
                <Text type="secondary">最近 96 个分钟点，用于观察日志是否持续进入系统</Text>
              </div>
              <Tag color="blue">{trendBars.length} 点</Tag>
            </div>
            <div className="trend-chart" role="img" aria-label="接入吞吐趋势">
              {trendBars.length > 0 ? (
                trendBars.map((point) => (
                  <span
                    key={point.bucket_minute}
                    title={`${point.bucket_minute} ${point.total} 条`}
                    style={{ height: `${Math.max(4, (point.total / trendMax) * 100)}%` }}
                  />
                ))
              ) : (
                <Text type="secondary">暂无分钟指标</Text>
              )}
            </div>
          </div>
        </Col>
        <Col xs={24} xl={9}>
          <div className="panel">
            <div className="panel-header">
              <div>
                <strong>需要关注</strong>
                <Text type="secondary">失败来源、待准入与解析器诊断</Text>
              </div>
            </div>
            <Space direction="vertical" size={10} style={{ width: '100%' }}>
              <button className="overview-link" type="button" onClick={() => onNavigate('logs', { include_failed: 'true' })}>
                失败/部分解析日志 <Tag>{failed}</Tag>
              </button>
              <button className="overview-link" type="button" onClick={() => onNavigate('sources')}>
                待准入来源 <Tag color={pendingAdmission ? 'warning' : 'default'}>{pendingAdmission}</Tag>
              </button>
              <button className="overview-link" type="button" onClick={() => onNavigate('parser')}>
                解析器诊断 <Tag color={parserDiagnostics.length ? 'warning' : 'default'}>{parserDiagnostics.length}</Tag>
              </button>
              <button className="overview-link" type="button" onClick={() => onNavigate('assets')}>
                归档资产状态 <DatabaseOutlined />
              </button>
            </Space>
          </div>
        </Col>
      </Row>

      <Row gutter={[16, 16]}>
        <Col xs={24} xl={14}>
          <div className="panel">
            <div className="panel-header">
              <div>
                <strong>接收状态</strong>
                <Text type="secondary">最近接收日志，确认设备是否持续上报</Text>
              </div>
            </div>
            <ProTable<RecentStreamRow>
              rowKey="key"
              size="small"
              search={false}
              options={false}
              pagination={false}
              dataSource={streamRows}
              columns={streamColumns}
              scroll={{ x: 900, y: 360 }}
              cardProps={false}
            />
          </div>
        </Col>
        <Col xs={24} xl={10}>
          <div className="panel">
            <div className="panel-header">
              <div>
                <strong>异常来源</strong>
                <Text type="secondary">近 24 小时解析失败来源 Top</Text>
              </div>
            </div>
            <ProTable<SourceMetricPoint>
              rowKey="source_addr"
              size="small"
              search={false}
              options={false}
              pagination={false}
              dataSource={sourceRows}
              columns={[
                { title: '来源', dataIndex: 'source_addr', ellipsis: true, render: (_, row) => <span className="mono">{row.source_addr}</span> },
                { title: '总数', dataIndex: 'total', width: 80 },
                { title: '失败', dataIndex: 'failed', width: 80, render: (_, row) => <Tag color={row.failed ? 'error' : 'default'}>{row.failed}</Tag> },
                { title: '最近', dataIndex: 'last_seen', width: 170, render: (_, row) => <span className="mono">{fmt(row.last_seen)}</span> },
              ]}
              scroll={{ x: 700, y: 360 }}
              cardProps={false}
            />
          </div>
        </Col>
      </Row>
    </Space>
  );
};
