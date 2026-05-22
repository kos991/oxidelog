import { AlertOutlined, ThunderboltOutlined } from '@ant-design/icons';
import { ProTable } from '@ant-design/pro-components';
import type { ProColumns } from '@ant-design/pro-components';
import { Button, Col, Row, Space, Tabs, Tag, Typography } from 'antd';
import React from 'react';
import type { AdaptiveRule, ParserDiagnostic, ParserProfile, ParserScopeState } from '@/services/oxidelog';
import { MetricCard } from '../components/MetricCard';
import { AdaptiveRuleStatusTag } from '../components/StatusTags';
import { fmt, tableOptions } from '../utils';

const { Text } = Typography;

export type ParserPageProps = {
  loading: boolean;
  adaptiveRules: AdaptiveRule[];
  parserDiagnostics: ParserDiagnostic[];
  parserProfiles: ParserProfile[];
  parserScopes: ParserScopeState[];
  onEnableRule: (ruleId: string) => Promise<void>;
  onDisableRule: (ruleId: string) => Promise<void>;
};

export const ParserPage: React.FC<ParserPageProps> = ({
  loading,
  adaptiveRules,
  parserDiagnostics,
  parserProfiles,
  parserScopes,
  onEnableRule,
  onDisableRule,
}) => {
  const activeCount = adaptiveRules.filter((rule) => rule.status === 'active').length;
  const shadowCount = adaptiveRules.filter((rule) => rule.status === 'shadow' || rule.status === 'shadow_recovering').length;
  const disabledCount = adaptiveRules.filter((rule) => rule.status === 'disabled').length;
  const quarantinedCount = parserScopes.filter((scope) => scope.adaptive_quarantine_until && new Date(scope.adaptive_quarantine_until) > new Date()).length;

  const ruleColumns: ProColumns<AdaptiveRule>[] = [
    { title: '规则 ID', dataIndex: 'rule_id', ellipsis: true, copyable: true },
    { title: 'Scope', dataIndex: 'scope_key', ellipsis: true },
    { title: '原始 Key', dataIndex: 'raw_key', ellipsis: true },
    { title: '标准字段', dataIndex: 'canonical_field', width: 120 },
    { title: '值类型', dataIndex: 'value_type', width: 100 },
    { title: '状态', dataIndex: 'status', width: 130, render: (_, row) => <AdaptiveRuleStatusTag status={row.status} /> },
    { title: '置信度', dataIndex: 'confidence', width: 100, render: (_, row) => <Text>{((row.confidence || 0) * 100).toFixed(1)}%</Text> },
    { title: '获胜/样本', width: 110, render: (_, row) => `${row.wins || 0}/${row.sample_count || 0}` },
    { title: '禁用原因', dataIndex: 'disabled_reason', ellipsis: true, render: (_, row) => fmt(row.disabled_reason) },
    {
      title: '操作',
      valueType: 'option',
      width: 120,
      render: (_, row) =>
        row.status === 'disabled'
          ? [<Button key="enable" size="small" type="link" onClick={() => onEnableRule(row.rule_id)}>启用</Button>]
          : [<Button key="disable" size="small" type="link" danger onClick={() => onDisableRule(row.rule_id)}>禁用</Button>],
    },
  ];

  return (
    <Space direction="vertical" size={16} className="page-stack">
      <Row gutter={[16, 16]}>
        <Col xs={24} md={6}><MetricCard title="活跃规则" value={activeCount} tone="success" icon={<ThunderboltOutlined />} /></Col>
        <Col xs={24} md={6}><MetricCard title="Shadow 规则" value={shadowCount} icon={<ThunderboltOutlined />} /></Col>
        <Col xs={24} md={6}><MetricCard title="禁用规则" value={disabledCount} tone={disabledCount ? 'warning' : 'default'} icon={<AlertOutlined />} /></Col>
        <Col xs={24} md={6}><MetricCard title="Quarantine Scope" value={quarantinedCount} tone={quarantinedCount ? 'danger' : 'default'} icon={<AlertOutlined />} /></Col>
      </Row>
      <div className="panel">
        <Tabs
          type="card"
          items={[
            {
              key: 'rules',
              label: '规则',
              children: (
                <ProTable<AdaptiveRule>
                  loading={loading}
                  headerTitle="自适应规则"
                  columns={ruleColumns}
                  dataSource={adaptiveRules}
                  rowKey="rule_id"
                  pagination={{ defaultPageSize: 20 }}
                  search={false}
                  options={tableOptions}
                  scroll={{ x: 1320, y: 560 }}
                  cardProps={false}
                />
              ),
            },
            {
              key: 'diagnostics',
              label: '诊断',
              children: (
                <ProTable<ParserDiagnostic>
                  loading={loading}
                  headerTitle="解析诊断"
                  columns={[
                    { title: 'Fingerprint', dataIndex: 'fingerprint', ellipsis: true, copyable: true },
                    { title: 'Scope', dataIndex: 'scope_key', ellipsis: true },
                    { title: '原因', dataIndex: 'reason', ellipsis: true },
                    { title: '样本原始行', dataIndex: 'sample_raw', ellipsis: true, copyable: true },
                    { title: '次数', dataIndex: 'count', width: 90, sorter: true },
                    { title: '最后出现', dataIndex: 'last_seen', width: 210, render: (_, row) => <span className="mono">{fmt(row.last_seen)}</span> },
                  ]}
                  dataSource={parserDiagnostics}
                  rowKey="fingerprint"
                  pagination={{ defaultPageSize: 20 }}
                  search={false}
                  options={tableOptions}
                  scroll={{ x: 1180, y: 560 }}
                  cardProps={false}
                />
              ),
            },
            {
              key: 'profiles',
              label: 'Profiles',
              children: (
                <ProTable<ParserProfile>
                  loading={loading}
                  headerTitle="解析器 Profile"
                  columns={[
                    { title: 'Scope', dataIndex: 'scope_key', ellipsis: true },
                    { title: '解析器 ID', dataIndex: 'parser_id', ellipsis: true },
                    { title: '名称', dataIndex: 'parser_name', ellipsis: true },
                    { title: '成功', dataIndex: 'success_count', width: 90, sorter: true },
                    { title: '部分', dataIndex: 'partial_count', width: 90, sorter: true },
                    { title: '失败', dataIndex: 'fail_count', width: 90, sorter: true },
                    { title: '最后出现', dataIndex: 'last_seen', width: 210, render: (_, row) => <span className="mono">{fmt(row.last_seen)}</span> },
                  ]}
                  dataSource={parserProfiles}
                  rowKey={(row) => `${row.scope_key}-${row.parser_id}`}
                  pagination={{ defaultPageSize: 20 }}
                  search={false}
                  options={tableOptions}
                  scroll={{ x: 1080, y: 560 }}
                  cardProps={false}
                />
              ),
            },
            {
              key: 'scopes',
              label: 'Scopes',
              children: (
                <ProTable<ParserScopeState>
                  loading={loading}
                  headerTitle="Scope 状态"
                  columns={[
                    { title: 'Scope', dataIndex: 'scope_key', ellipsis: true },
                    { title: '自适应学习', dataIndex: 'adaptive_learning_enabled', width: 120, render: (_, row) => <Tag color={row.adaptive_learning_enabled ? 'green' : 'red'}>{row.adaptive_learning_enabled ? '启用' : '停用'}</Tag> },
                    { title: 'Quarantine', dataIndex: 'adaptive_quarantine_until', width: 210, render: (_, row) => row.adaptive_quarantine_until ? <span className="mono">{fmt(row.adaptive_quarantine_until)}</span> : '-' },
                    { title: 'Metrics Gap', dataIndex: 'metrics_gap', width: 120, render: (_, row) => <Tag color={row.metrics_gap ? 'orange' : 'default'}>{row.metrics_gap ? '有缺口' : '正常'}</Tag> },
                    { title: '最后出现', dataIndex: 'last_seen', width: 210, render: (_, row) => <span className="mono">{fmt(row.last_seen)}</span> },
                  ]}
                  dataSource={parserScopes}
                  rowKey="scope_key"
                  pagination={{ defaultPageSize: 20 }}
                  search={false}
                  options={tableOptions}
                  scroll={{ x: 1120, y: 560 }}
                  cardProps={false}
                />
              ),
            },
          ]}
        />
      </div>
    </Space>
  );
};
