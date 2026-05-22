import { CloudServerOutlined, PlusOutlined, ReloadOutlined, SafetyCertificateOutlined } from '@ant-design/icons';
import { ProTable } from '@ant-design/pro-components';
import type { ProColumns } from '@ant-design/pro-components';
import { Button, Col, Row, Space, Tag, Typography } from 'antd';
import React from 'react';
import type { AdmissionCase, CustomIpRegion, DeviceProfile } from '@/services/oxidelog';
import { MetricCard } from '../components/MetricCard';
import { AdmissionStateTag, DeviceStateTag } from '../components/StatusTags';
import type { AdmissionAction, DeviceRow } from '../types';
import { fmt, shortHash, tableOptions } from '../utils';

const { Text } = Typography;

export type SourceGovernancePageProps = {
  loading: boolean;
  deviceRows: DeviceRow[];
  customIpRegions: CustomIpRegion[];
  admissionCaseRows: AdmissionCase[];
  admissionProfileRows: DeviceProfile[];
  onBackfillDevices: () => Promise<void>;
  onOpenDeviceModal: (row?: DeviceRow) => void;
  onToggleDevice: (row: DeviceRow) => Promise<void>;
  onDeleteDevice: (row: DeviceRow) => Promise<void>;
  onOpenIpRegionModal: (row?: CustomIpRegion) => void;
  onToggleCustomIpRegion: (row: CustomIpRegion) => Promise<void>;
  onDeleteCustomIpRegion: (row: CustomIpRegion) => Promise<void>;
  onRefreshAdmission: () => Promise<void>;
  onApproveAdmissionCase: AdmissionAction;
  onBlockAdmissionCase: AdmissionAction;
  onReopenAdmissionCase: AdmissionAction;
  onViewLogs: (filters: Record<string, string>) => void;
};

export const SourceGovernancePage: React.FC<SourceGovernancePageProps> = ({
  loading,
  deviceRows,
  customIpRegions,
  admissionCaseRows,
  admissionProfileRows,
  onBackfillDevices,
  onOpenDeviceModal,
  onToggleDevice,
  onDeleteDevice,
  onOpenIpRegionModal,
  onToggleCustomIpRegion,
  onDeleteCustomIpRegion,
  onRefreshAdmission,
  onApproveAdmissionCase,
  onBlockAdmissionCase,
  onReopenAdmissionCase,
  onViewLogs,
}) => {
  const pending = admissionCaseRows.filter((row) => row.state === 'pending').length;
  const trusted = admissionCaseRows.filter((row) => row.state === 'trusted').length;
  const blocked = admissionCaseRows.filter((row) => row.state === 'blocked').length;

  const deviceColumns: ProColumns<DeviceRow>[] = [
    { title: '对象', dataIndex: 'name', width: 180, render: (_, row) => row.name || row.device },
    { title: '接入地址', dataIndex: 'device', width: 190, copyable: true, render: (_, row) => <span className="mono">{row.device}</span> },
    { title: '类型', dataIndex: 'kind', width: 110, render: (_, row) => <Tag color={row.kind === 'managed' ? 'blue' : 'default'}>{row.kind === 'managed' ? '纳管设备' : '观察来源'}</Tag> },
    { title: '状态', dataIndex: 'enabled', width: 100, render: (_, row) => <DeviceStateTag managed={row.kind === 'managed'} enabled={row.enabled} /> },
    { title: '协议', dataIndex: 'protocol', width: 90 },
    { title: '端口', dataIndex: 'port', width: 80, render: (_, row) => row.port || '-' },
    { title: '日志量', dataIndex: 'total', width: 100, sorter: (a, b) => a.total - b.total },
    { title: '失败', dataIndex: 'failed', width: 86, render: (_, row) => <Tag color={row.failed ? 'error' : 'default'}>{row.failed}</Tag> },
    { title: '最近日志', dataIndex: 'lastTime', width: 210, render: (_, row) => <span className="mono">{row.lastTime}</span> },
    {
      title: '操作',
      valueType: 'option',
      width: 280,
      render: (_, row) => [
        <Button key="view" size="small" type="link" onClick={() => onViewLogs({ device: row.sourceQuery || row.device })}>
          查看日志
        </Button>,
        row.kind === 'managed' ? (
          <Button key="edit" size="small" type="link" onClick={() => onOpenDeviceModal(row)}>编辑</Button>
        ) : (
          <Button key="adopt" size="small" type="link" onClick={() => onOpenDeviceModal(row)}>纳管</Button>
        ),
        row.kind === 'managed' ? (
          <Button key="toggle" size="small" type="link" onClick={() => onToggleDevice(row)}>{row.enabled ? '停用' : '启用'}</Button>
        ) : null,
        row.kind === 'managed' ? (
          <Button key="delete" size="small" type="link" danger onClick={() => onDeleteDevice(row)}>删除</Button>
        ) : null,
      ],
    },
  ];

  return (
    <Space direction="vertical" size={16} className="page-stack">
      <Row gutter={[16, 16]}>
        <Col xs={24} md={6}><MetricCard title="来源对象" value={deviceRows.length} icon={<CloudServerOutlined />} /></Col>
        <Col xs={24} md={6}><MetricCard title="待准入" value={pending} tone={pending ? 'warning' : 'default'} icon={<SafetyCertificateOutlined />} /></Col>
        <Col xs={24} md={6}><MetricCard title="已信任" value={trusted} tone="success" icon={<SafetyCertificateOutlined />} /></Col>
        <Col xs={24} md={6}><MetricCard title="已阻断" value={blocked} tone={blocked ? 'danger' : 'default'} icon={<SafetyCertificateOutlined />} /></Col>
      </Row>

      <div className="panel">
        <div className="panel-header">
          <div>
            <strong>设备与来源</strong>
            <Text type="secondary">纳管设备、观察来源与日志接入健康放在同一张表</Text>
          </div>
          <Space>
            <Button type="primary" icon={<PlusOutlined />} onClick={() => onOpenDeviceModal()}>添加设备</Button>
            <Button loading={loading} onClick={onBackfillDevices}>回填设备 ID</Button>
          </Space>
        </div>
        <ProTable<DeviceRow>
          rowKey="device"
          size="small"
          dataSource={deviceRows}
          search={false}
          options={tableOptions}
          pagination={{ pageSize: 50, showSizeChanger: true }}
          columns={deviceColumns}
          scroll={{ x: 1280, y: 420 }}
          cardProps={false}
        />
      </div>

      <div className="panel">
        <div className="panel-header">
          <div>
            <strong>准入控制</strong>
            <Text type="secondary">未知来源审批、信任和阻断历史</Text>
          </div>
          <Button loading={loading} icon={<ReloadOutlined />} onClick={onRefreshAdmission}>刷新准入</Button>
        </div>
        <ProTable<AdmissionCase>
          rowKey="case_id"
          size="small"
          dataSource={admissionCaseRows}
          search={false}
          options={tableOptions}
          pagination={{ pageSize: 20, showSizeChanger: true }}
          columns={[
            { title: '状态', dataIndex: 'state', width: 100, render: (_, row) => <AdmissionStateTag state={row.state} /> },
            { title: '来源', dataIndex: 'source_ip', width: 190, copyable: true, render: (_, row) => <span className="mono">{`${row.transport}/${row.source_ip}/${row.listen_port}`}</span> },
            { title: '指纹', dataIndex: 'fingerprint_hash', width: 150, copyable: true, render: (_, row) => <span className="mono">{shortHash(row.fingerprint_hash)}</span> },
            { title: '厂商', dataIndex: 'vendor_hint', width: 120, render: (_, row) => <Tag color="blue">{fmt(row.vendor_hint || row.common_profile)}</Tag> },
            { title: '评分', dataIndex: 'score', width: 80, sorter: (a, b) => a.score - b.score },
            { title: '次数', dataIndex: 'seen_count', width: 80, sorter: (a, b) => a.seen_count - b.seen_count },
            { title: '最后出现', dataIndex: 'last_seen', width: 210, render: (_, row) => <span className="mono">{fmt(row.last_seen)}</span> },
            { title: '原因', dataIndex: 'reason', ellipsis: true },
            {
              title: '操作',
              valueType: 'option',
              width: 220,
              render: (_, row) => [
                row.state !== 'trusted' ? <Button key="approve" size="small" type="link" onClick={() => onApproveAdmissionCase(row)}>批准</Button> : null,
                row.state !== 'blocked' ? <Button key="block" size="small" type="link" danger onClick={() => onBlockAdmissionCase(row)}>阻断</Button> : null,
                row.state === 'blocked' ? <Button key="reopen" size="small" type="link" onClick={() => onReopenAdmissionCase(row)}>重开</Button> : null,
              ],
            },
          ]}
          scroll={{ x: 1320, y: 360 }}
          cardProps={false}
        />
      </div>

      <div className="panel two-column-panel">
        <div>
          <div className="panel-header">
            <div>
              <strong>自定义 IP 归属地</strong>
              <Text type="secondary">用于日志结果里的归属地标注</Text>
            </div>
            <Button icon={<PlusOutlined />} onClick={() => onOpenIpRegionModal()}>添加网段</Button>
          </div>
          <ProTable<CustomIpRegion>
            rowKey="id"
            size="small"
            dataSource={customIpRegions}
            search={false}
            options={false}
            pagination={{ pageSize: 10 }}
            columns={[
              { title: 'CIDR', dataIndex: 'cidr', width: 170, copyable: true },
              { title: '名称', dataIndex: 'name', width: 180 },
              { title: '状态', dataIndex: 'enabled', width: 90, render: (_, row) => row.enabled ? <Tag color="success">启用</Tag> : <Tag>停用</Tag> },
              {
                title: '操作',
                valueType: 'option',
                width: 170,
                render: (_, row) => [
                  <Button key="edit" size="small" type="link" onClick={() => onOpenIpRegionModal(row)}>编辑</Button>,
                  <Button key="toggle" size="small" type="link" onClick={() => onToggleCustomIpRegion(row)}>{row.enabled ? '停用' : '启用'}</Button>,
                  <Button key="delete" size="small" type="link" danger onClick={() => onDeleteCustomIpRegion(row)}>删除</Button>,
                ],
              },
            ]}
            scroll={{ x: 720, y: 260 }}
            cardProps={false}
          />
        </div>
        <div>
          <div className="panel-header">
            <div>
              <strong>信任 Profile</strong>
              <Text type="secondary">已批准来源的指纹历史</Text>
            </div>
          </div>
          <ProTable<DeviceProfile>
            rowKey="device_id"
            size="small"
            dataSource={admissionProfileRows}
            search={false}
            options={false}
            pagination={{ pageSize: 10 }}
            columns={[
              { title: '设备 ID', dataIndex: 'device_id', width: 180, copyable: true },
              { title: '状态', dataIndex: 'state', width: 100, render: (_, row) => <AdmissionStateTag state={row.state} /> },
              { title: '来源 IP', dataIndex: 'source_ip', width: 140, copyable: true },
              { title: '更新时间', dataIndex: 'updated_at', width: 190, render: (_, row) => <span className="mono">{fmt(row.updated_at)}</span> },
            ]}
            scroll={{ x: 760, y: 260 }}
            cardProps={false}
          />
        </div>
      </div>
    </Space>
  );
};
