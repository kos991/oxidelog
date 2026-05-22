import {
  CloudServerOutlined,
  DownloadOutlined,
  FileSearchOutlined,
  HistoryOutlined,
  SearchOutlined,
} from '@ant-design/icons';
import { ProForm, ProFormSelect, ProFormText, ProTable } from '@ant-design/pro-components';
import type { ProColumns, ProFormInstance } from '@ant-design/pro-components';
import { Alert, Button, DatePicker, Dropdown, Space, Switch, Tag, Typography, message } from 'antd';
import type { Dayjs } from 'dayjs';
import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  type ArchiveFile,
  type CanonicalEvent,
  type ColdSearchResult,
  type ExportJob,
  type UnifiedSearchRow,
  createExportJob,
  fetchExportJob,
  fetchIpRegion,
  searchExportUrl,
  searchUnified,
} from '@/services/oxidelog';
import { ParseStatusTag } from '../components/StatusTags';
import { normalizeSearchValues } from '../searchParams';
import {
  bytes,
  downloadUrl,
  fmt,
  ipWithRegion,
  isPublicIp,
  localDay,
  protocolTag,
  tableOptions,
  type SearchResultRow,
} from '../utils';

const { Text } = Typography;
const { RangePicker } = DatePicker;

type SearchFormValues = Record<string, unknown>;
type DownloadFormat = 'csv' | 'zst' | 'parquet';

export type LogSearchPanelProps = {
  events: CanonicalEvent[];
  loading?: boolean;
  archiveFiles: ArchiveFile[];
  frozenFiles: ArchiveFile[];
  deviceFilterOptions: { label: string; value: string }[];
  hotLogDaySet: Set<string>;
  coldLogDaySet: Set<string>;
  displayDeviceName: (source?: string | null, raw?: string | null) => string;
  initialFilters?: Record<string, string>;
};

const dateSpanDays = (values: Record<string, string>) => {
  if (!values.date_from || !values.date_to) return 0;
  const start = new Date(`${values.date_from}T00:00:00`);
  const end = new Date(`${values.date_to}T00:00:00`);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return 0;
  return Math.floor((end.getTime() - start.getTime()) / 86400000) + 1;
};

const isBulkQuery = (values: Record<string, string>) => dateSpanDays(values) >= 180;

export const LogSearchPage: React.FC<LogSearchPanelProps> = ({
  events,
  loading: externalLoading,
  archiveFiles,
  frozenFiles,
  deviceFilterOptions,
  hotLogDaySet,
  coldLogDaySet,
  displayDeviceName,
  initialFilters,
}) => {
  const [logFilters, setLogFilters] = useState<Record<string, string>>(initialFilters || {});
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [unifiedResults, setUnifiedResults] = useState<UnifiedSearchRow[]>([]);
  const [hotResults, setHotResults] = useState<CanonicalEvent[]>([]);
  const [cold, setCold] = useState<ColdSearchResult>();
  const [exportLoading, setExportLoading] = useState(false);
  const [exportJobs, setExportJobs] = useState<ExportJob[]>([]);
  const [ipRegions, setIpRegions] = useState<Record<string, string>>({});
  const [searchLoading, setSearchLoading] = useState(false);
  const searchFormRef = useRef<ProFormInstance<SearchFormValues> | undefined>(undefined);

  useEffect(() => {
    setHotResults(events);
    setUnifiedResults(events.map((event) => ({ result_source: 'hot', event })));
    setCold(undefined);
  }, [events]);

  useEffect(() => {
    if (initialFilters && Object.keys(initialFilters).length > 0) {
      setLogFilters(initialFilters);
      searchFormRef.current?.setFieldsValue(initialFilters);
    }
  }, [initialFilters]);

  const actionFilterOptions = useMemo(
    () =>
      Array.from(
        new Set(
          [
            'snat',
            'dnat',
            'allow',
            'deny',
            ...events.map((event) => fmt(event.action).toLowerCase()).filter((value) => value !== '-'),
            ...(cold?.events || []).map((event) => fmt(event.action).toLowerCase()).filter((value) => value !== '-'),
          ].filter(Boolean),
        ),
      ).map((value) => ({ label: value, value })),
    [events, cold],
  );

  const getSearchValues = () => {
    const values = (searchFormRef.current?.getFieldsValue?.() || {}) as SearchFormValues;
    return normalizeSearchValues(values);
  };

  const createDownloadJob = async (values: Record<string, string>, format: DownloadFormat) => {
    setExportLoading(true);
    try {
      const job = await createExportJob({
        scope: values.scope || 'all',
        limit: String(values.limit || 1000000),
        format,
        ...values,
      });
      setExportJobs((current) => [job, ...current.filter((item) => item.job_id !== job.job_id)]);
      message.success('已创建导出任务，完成后可在任务区下载');
      const poll = async () => {
        const next = await fetchExportJob(job.job_id);
        setExportJobs((current) => [next, ...current.filter((item) => item.job_id !== next.job_id)]);
        if (next.status === 'queued' || next.status === 'running') window.setTimeout(poll, 2000);
      };
      window.setTimeout(poll, 1000);
    } catch (error) {
      message.error(error instanceof Error ? error.message : '创建导出任务失败');
    } finally {
      setExportLoading(false);
    }
  };

  const exportByFormat = async (format: DownloadFormat) => {
    const values: Record<string, string> = { ...getSearchValues(), scope: 'all' };
    if (format === 'csv' && !isBulkQuery(values)) {
      downloadUrl(searchExportUrl(Number(values.limit || 1000000), values), '日志检索结果.csv');
      return;
    }
    await createDownloadJob(values, format);
  };

  const oneYearValues = () => {
    const values = getSearchValues();
    const end = new Date();
    const start = new Date(end);
    start.setFullYear(start.getFullYear() - 1);
    delete values.day;
    values.date_from = localDay(start);
    values.date_to = localDay(end);
    values.scope = 'all';
    return values;
  };

  const dateCellRender = (current: Dayjs) => {
    const day = current.format('YYYY-MM-DD');
    const hasHot = hotLogDaySet.has(day);
    const hasCold = coldLogDaySet.has(day);
    return (
      <div className="log-date-cell">
        <span>{current.date()}</span>
        <span className="log-date-dots">
          {hasHot ? <i className="log-date-dot hot" /> : null}
          {hasCold ? <i className="log-date-dot cold" /> : null}
        </span>
      </div>
    );
  };

  const onSearch = async (scope: 'hot' | 'archive' | 'all') => {
    const values = getSearchValues();
    setLogFilters(values);
    try {
      setSearchLoading(true);
      const result = await searchUnified({ ...values, scope, limit: values.limit || '500' });
      setUnifiedResults(result);
      setHotResults(result.filter((row) => row.result_source === 'hot').map((row) => row.event));
      const archiveEvents = result.filter((row) => row.result_source !== 'hot').map((row) => row.event);
      setCold(
        scope === 'hot'
          ? undefined
          : {
              files: archiveFiles.length + frozenFiles.length,
              scanned_lines: 0,
              matched: archiveEvents.length,
              limited: result.length >= Number(values.limit || 500),
              events: archiveEvents,
            },
      );
    } catch (error) {
      message.error(error instanceof Error ? error.message : '日志检索失败');
    } finally {
      setSearchLoading(false);
    }
  };

  const searchResultRows = useMemo<SearchResultRow[]>(
    () =>
      unifiedResults.map((row, index) => ({
        ...row.event,
        result_key: `${row.result_source}-${row.event.event_id}-${index}`,
        result_source: row.result_source === 'hot' ? '热库' : '归档',
        archive_path: row.archive_path,
        device_name: row.device_name,
        geo_region: row.geo_region,
        src_geo_region: row.src_geo_region,
        dst_geo_region: row.dst_geo_region,
      })),
    [unifiedResults],
  );

  useEffect(() => {
    const pendingIps = Array.from(
      new Set(
        searchResultRows
          .flatMap((event) => [fmt(event.src_ip), fmt(event.dst_ip)])
          .filter((ip) => {
            const rowRegion = searchResultRows.find((row) => row.src_ip === ip || row.dst_ip === ip);
            const hasRegion =
              (rowRegion?.src_ip === ip && rowRegion.src_geo_region) ||
              (rowRegion?.dst_ip === ip && rowRegion.dst_geo_region);
            return isPublicIp(ip) && !hasRegion && !ipRegions[ip];
          }),
      ),
    );
    if (pendingIps.length === 0) return;
    let cancelled = false;
    Promise.all(
      pendingIps.map(async (ip) => {
        try {
          const result = await fetchIpRegion(ip);
          return [ip, result.region || '公网归属地待识别'] as const;
        } catch {
          return [ip, '公网归属地待识别'] as const;
        }
      }),
    ).then((rows) => {
      if (cancelled) return;
      setIpRegions((current) => {
        const next = { ...current };
        rows.forEach(([ip, region]) => {
          next[ip] = region;
        });
        return next;
      });
    });
    return () => {
      cancelled = true;
    };
  }, [ipRegions, searchResultRows]);

  const columns: ProColumns<SearchResultRow>[] = [
    { title: '来源', dataIndex: 'result_source', width: 86, fixed: 'left', render: (_, row) => <Tag color={row.result_source === '热库' ? 'blue' : 'purple'}>{row.result_source}</Tag> },
    { title: '接收时间', dataIndex: 'ingest_time', width: 190, valueType: 'dateTime', render: (_, row) => <span className="mono">{fmt(row.ingest_time)}</span> },
    { title: '状态', dataIndex: 'parse_status', width: 110, render: (_, row) => <ParseStatusTag status={row.parse_status} /> },
    {
      title: '设备来源',
      dataIndex: 'source_addr',
      width: 220,
      copyable: true,
      render: (_, row) => (
        <Space size={6}>
          <CloudServerOutlined className="muted-icon" />
          <span className="mono">{row.device_name || displayDeviceName(row.source_addr, row.raw)}</span>
        </Space>
      ),
    },
    { title: '源地址', dataIndex: 'src_ip', width: 210, copyable: true, render: (_, row) => ipWithRegion(row.src_ip, row.src_geo_region || ipRegions[fmt(row.src_ip)]) },
    { title: '源端口', dataIndex: 'src_port', width: 90, render: (_, row) => <span className="mono">{fmt(row.src_port)}</span> },
    { title: '目的地址', dataIndex: 'dst_ip', width: 230, copyable: true, render: (_, row) => ipWithRegion(row.dst_ip, row.dst_geo_region || ipRegions[fmt(row.dst_ip)]) },
    { title: '目的端口', dataIndex: 'dst_port', width: 96, render: (_, row) => <span className="mono">{fmt(row.dst_port)}</span> },
    { title: '协议', dataIndex: 'protocol', width: 82, render: (_, row) => protocolTag(row.protocol) },
    { title: '动作', dataIndex: 'action', width: 90, render: (_, row) => <Tag>{fmt(row.action)}</Tag> },
    { title: '原始日志', dataIndex: 'raw', ellipsis: true, copyable: true, render: (_, row) => <span className="mono raw-log">{fmt(row.raw)}</span> },
    { title: '归档路径', dataIndex: 'archive_path', ellipsis: true, copyable: true, render: (_, row) => <span className="mono">{fmt(row.archive_path)}</span> },
  ];

  const loading = externalLoading || searchLoading;
  const currentValues = getSearchValues();
  const downloadFormats = isBulkQuery(currentValues)
    ? [{ key: 'zst', label: 'ZST' }, { key: 'parquet', label: 'Parquet' }]
    : [{ key: 'csv', label: 'CSV' }, { key: 'zst', label: 'ZST' }, { key: 'parquet', label: 'Parquet' }];

  return (
    <div className="log-workbench">
      <aside className="log-filter-rail">
        <div className="panel-header">
          <div>
            <strong>日志检索</strong>
            <Text type="secondary">默认只保留 IP 与时间范围</Text>
          </div>
          <Switch checked={advancedOpen} onChange={setAdvancedOpen} checkedChildren="高级" unCheckedChildren="高级" />
        </div>
        <ProForm
          formRef={searchFormRef}
          layout="vertical"
          className="log-search-form"
          submitter={false}
          initialValues={{ limit: '500', include_failed: 'true', ...logFilters }}
        >
          <ProForm.Item name="date_range" label="时间范围">
            <RangePicker
              allowClear
              format="YYYY-MM-DD"
              placeholder={['开始日期', '结束日期']}
              cellRender={(current) => dateCellRender(current as Dayjs)}
              popupClassName="log-date-picker"
              style={{ width: '100%' }}
            />
          </ProForm.Item>
          <div className="log-date-legend">
            <i className="log-date-dot hot" /> 热库
            <i className="log-date-dot cold" /> 冷库
          </div>
          <ProFormText name="src_ip" label="源 IP" placeholder="2.55.80.6" />
          <ProFormText name="dst_ip" label="目的 IP" placeholder="211.93.49.88" />
          {advancedOpen ? (
            <>
              <ProFormSelect name="device" label="设备来源" placeholder="选择设备" showSearch options={deviceFilterOptions} fieldProps={{ allowClear: true }} />
              <ProFormText name="src_port" label="源端口" placeholder="50000" />
              <ProFormText name="dst_port" label="目的端口" placeholder="443" />
              <ProFormSelect name="protocol" label="协议" placeholder="全部" options={[{ label: 'UDP', value: 'UDP' }, { label: 'TCP', value: 'TCP' }]} fieldProps={{ allowClear: true }} />
              <ProFormSelect name="action" label="动作" placeholder="全部" showSearch options={actionFilterOptions} fieldProps={{ allowClear: true }} />
              <ProFormText name="keyword" label="关键字" placeholder="NAT 日志关键字" />
              <ProFormText name="limit" label="返回上限" placeholder="500" />
              <ProFormSelect name="include_failed" label="解析失败日志" options={[{ label: '包含失败', value: 'true' }, { label: '隐藏失败', value: 'false' }]} />
            </>
          ) : null}
        </ProForm>
        <Space direction="vertical" size={8} style={{ width: '100%' }}>
          <Button block type="primary" icon={<SearchOutlined />} loading={loading} onClick={() => onSearch('hot')}>
            查询热库
          </Button>
          <Button block icon={<HistoryOutlined />} loading={loading} onClick={() => onSearch('archive')}>
            追溯归档
          </Button>
          <Button block icon={<FileSearchOutlined />} loading={loading} onClick={() => onSearch('all')}>
            全量检索
          </Button>
          <Button
            block
            onClick={() => {
              searchFormRef.current?.resetFields();
              setLogFilters({});
              setCold(undefined);
              setHotResults(events);
              setUnifiedResults(events.map((event) => ({ result_source: 'hot', event })));
            }}
          >
            重置
          </Button>
        </Space>
      </aside>

      <section className="log-results-pane">
        <Alert
          className="history-alert"
          type="info"
          showIcon
          message="查询使用热库、归档或全量范围；导出基于当前查询条件创建，下载时选择 CSV、ZST 或 Parquet。半年/一年这类大范围只提供 ZST 和 Parquet。"
        />
        <ProTable<SearchResultRow>
          rowKey="result_key"
          headerTitle="查询结果"
          size="small"
          loading={loading}
          dataSource={searchResultRows}
          columns={columns}
          search={false}
          options={tableOptions}
          pagination={{ pageSize: 50, showSizeChanger: true }}
          scroll={{ x: 1680, y: 620 }}
          cardBordered={false}
          toolBarRender={() => [
            <Text key="summary" type="secondary">
              当前 {hotResults.length} 条{cold ? `，归档 ${cold.events.length} 条，命中 ${cold.matched} 条` : ''}{cold?.limited ? '，已截断' : ''}
            </Text>,
            <Dropdown
              key="export"
              menu={{
                items: downloadFormats,
                onClick: ({ key }) => exportByFormat(key as DownloadFormat),
              }}
            >
              <Button icon={<DownloadOutlined />} loading={exportLoading} disabled={searchResultRows.length === 0}>
                创建导出
              </Button>
            </Dropdown>,
            <Dropdown
              key="export-year"
              menu={{
                items: [{ key: 'zst', label: 'ZST' }, { key: 'parquet', label: 'Parquet' }],
                onClick: ({ key }) => createDownloadJob(oneYearValues(), key as DownloadFormat),
              }}
            >
              <Button icon={<DownloadOutlined />} loading={exportLoading}>
                一年日志
              </Button>
            </Dropdown>,
          ]}
        />
        {exportJobs.length > 0 ? (
          <div className="export-task-panel">
            <div className="panel-header">
              <div>
                <strong>导出任务</strong>
                <Text type="secondary">格式在创建任务时确定</Text>
              </div>
            </div>
            <ProTable<ExportJob>
              rowKey="job_id"
              size="small"
              search={false}
              options={false}
              pagination={false}
              dataSource={exportJobs.slice(0, 6)}
              columns={[
                { title: '文件', dataIndex: 'file_name', ellipsis: true },
                { title: '状态', dataIndex: 'status', width: 100, render: (_, row) => <Tag color={row.status === 'completed' ? 'success' : row.status === 'failed' ? 'error' : 'processing'}>{row.status}</Tag> },
                { title: '行数', dataIndex: 'rows', width: 100 },
                { title: '大小', dataIndex: 'file_bytes', width: 120, render: (_, row) => bytes(row.file_bytes) },
                { title: '格式', dataIndex: 'format', width: 90, render: (_, row) => <Tag>{(row.format || 'zst').toUpperCase()}</Tag> },
                {
                  title: '下载',
                  valueType: 'option',
                  width: 90,
                  render: (_, row) => (
                    <Button
                      size="small"
                      type="link"
                      disabled={row.status !== 'completed'}
                      onClick={() => downloadUrl(row.download_url, row.file_name)}
                    >
                      下载
                    </Button>
                  ),
                },
              ]}
              cardProps={false}
            />
          </div>
        ) : null}
      </section>
    </div>
  );
};

export default LogSearchPage;
