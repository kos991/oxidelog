import { DatabaseOutlined, FileSearchOutlined, HistoryOutlined, ReloadOutlined } from '@ant-design/icons';
import { ProTable } from '@ant-design/pro-components';
import type { ProColumns } from '@ant-design/pro-components';
import { Button, Col, Row, Space, Tag, Typography } from 'antd';
import React from 'react';
import { MetricCard } from '../components/MetricCard';
import type { ArchiveIndexTableRow, AssetRow } from '../types';
import { bytes, fmt, tableOptions } from '../utils';

const { Text } = Typography;

export type AssetsPageProps = {
  loading: boolean;
  assetRows: AssetRow[];
  archiveIndexRows: ArchiveIndexTableRow[];
  frozenCount: number;
  totalBytes: number;
  onRebuildArchiveIndex: () => Promise<void>;
};

const fileColumns: ProColumns<AssetRow>[] = [
  { title: '文件路径', dataIndex: 'path', copyable: true, render: (_, record) => <span className="mono">{fmt(record.path)}</span> },
  { title: '大小', dataIndex: 'bytes', width: 120, sorter: (a, b) => a.bytes - b.bytes, render: (_, record) => bytes(record.bytes) },
];

export const AssetsPage: React.FC<AssetsPageProps> = ({
  loading,
  assetRows,
  archiveIndexRows,
  frozenCount,
  totalBytes,
  onRebuildArchiveIndex,
}) => (
  <Space direction="vertical" size={16} className="page-stack">
    <Row gutter={[16, 16]}>
      <Col xs={24} md={8}><MetricCard title="日志存储总量" value={bytes(totalBytes)} icon={<DatabaseOutlined />} /></Col>
      <Col xs={24} md={8}><MetricCard title="归档文件数量" value={assetRows.length} icon={<FileSearchOutlined />} /></Col>
      <Col xs={24} md={8}><MetricCard title="Frozen 原始归档" value={frozenCount} icon={<HistoryOutlined />} /></Col>
    </Row>
    <div className="panel">
      <div className="panel-header">
        <div>
          <strong>归档资产</strong>
          <Text type="secondary">冷库文件与原始压缩归档</Text>
        </div>
      </div>
      <ProTable<AssetRow>
        rowKey="path"
        size="small"
        dataSource={assetRows}
        columns={[
          { title: '类型', dataIndex: 'category', width: 150, filters: true, onFilter: true, render: (_, record) => <Tag color={record.category === 'Parquet 冷库' ? 'blue' : 'purple'}>{record.category}</Tag> },
          ...fileColumns,
        ]}
        search={false}
        options={tableOptions}
        pagination={{ pageSize: 50, showSizeChanger: true }}
        scroll={{ x: 900, y: 460 }}
        cardProps={false}
      />
    </div>
    <div className="panel">
      <div className="panel-header">
        <div>
          <strong>Frozen 归档索引</strong>
          <Text type="secondary">用于归档追溯的日期和来源索引</Text>
        </div>
        <Button loading={loading} icon={<ReloadOutlined />} onClick={onRebuildArchiveIndex}>重建索引</Button>
      </div>
      <ProTable<ArchiveIndexTableRow>
        rowKey="key"
        size="small"
        dataSource={archiveIndexRows}
        columns={[
          { title: '日期', dataIndex: 'day', width: 130, render: (_, record) => <span className="mono">{record.day}</span> },
          { title: '归档路径', dataIndex: 'archive_path', copyable: true, ellipsis: true, render: (_, record) => <span className="mono">{record.archive_path}</span> },
          { title: '来源', dataIndex: 'source_addr', width: 220, copyable: true, render: (_, record) => <span className="mono">{record.source_addr}</span> },
          { title: '大小', dataIndex: 'bytes', width: 120, sorter: (a, b) => a.bytes - b.bytes, render: (_, record) => bytes(record.bytes) },
          { title: '行数', dataIndex: 'line_count', width: 100, sorter: (a, b) => a.line_count - b.line_count },
          { title: '索引时间', dataIndex: 'indexed_at', width: 220, render: (_, record) => <span className="mono">{fmt(record.indexed_at)}</span> },
        ]}
        search={false}
        options={tableOptions}
        pagination={{ pageSize: 50, showSizeChanger: true }}
        scroll={{ x: 1120, y: 360 }}
        cardProps={false}
      />
    </div>
  </Space>
);
