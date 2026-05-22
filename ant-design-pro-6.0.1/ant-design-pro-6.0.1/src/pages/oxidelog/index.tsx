import { ProForm, ProFormDigit, ProFormSelect, ProFormText } from '@ant-design/pro-components';
import { Alert, Modal, message } from 'antd';
import React, { useEffect, useMemo, useState } from 'react';
import {
  type AdmissionCase,
  type AdaptiveRule,
  type ArchiveFile,
  type ArchiveIndexRow,
  type CanonicalEvent,
  type CustomIpRegion,
  type DeviceProfile,
  type FirewallDevice,
  type HourMetricPoint,
  type MinuteMetricPoint,
  type ParseErrorSummary,
  type ParserDiagnostic,
  type ParserProfile,
  type ParserScopeState,
  type SourceMetricPoint,
  type SystemStatus,
  admissionCases,
  admissionProfiles,
  approveAdmissionCase,
  backfillDevices,
  blockAdmissionCase,
  createCustomIpRegion,
  createDevice,
  deleteCustomIpRegion,
  deleteDevice,
  disableAdaptiveRule,
  enableAdaptiveRule,
  fetchAdaptiveRules,
  fetchArchiveDays,
  fetchArchiveFiles,
  fetchArchiveIndex,
  fetchCustomIpRegions,
  fetchDevices,
  fetchEvents,
  fetchFrozenFiles,
  fetchHourMetrics,
  fetchMinuteMetrics,
  fetchParserDiagnostics,
  fetchParserProfiles,
  fetchParserScopes,
  fetchParserSummary,
  fetchSourceMetrics,
  fetchStatus,
  rebuildArchiveIndex,
  reopenAdmissionCase,
  updateCustomIpRegion,
  updateDevice,
} from '@/services/oxidelog';
import { OxideLogShell } from './components/Shell';
import LogSearchPanel from './LogSearchPanel';
import { AssetsPage } from './pages/AssetsPage';
import { OverviewPage } from './pages/OverviewPage';
import { ParserPage } from './pages/ParserPage';
import { SourceGovernancePage } from './pages/SourceGovernancePage';
import type { ArchiveIndexTableRow, AssetRow, DeviceFormState, DeviceRow, PageKey } from './types';
import {
  archiveDaysFromText,
  deviceName,
  deviceProtocol,
  deviceSourceKeys,
  fmt,
  managedProtocol,
} from './utils';
import './style.less';

const OxideLogPage: React.FC = () => {
  const [activePage, setActivePage] = useState<PageKey>('overview');
  const [collapsed, setCollapsed] = useState(true);
  const [loading, setLoading] = useState(false);
  const [systemStatus, setSystemStatus] = useState<SystemStatus>();
  const [logFilters, setLogFilters] = useState<Record<string, string>>({});

  const [events, setEvents] = useState<CanonicalEvent[]>([]);
  const [minuteMetrics, setMinuteMetrics] = useState<MinuteMetricPoint[]>([]);
  const [hourMetrics, setHourMetrics] = useState<HourMetricPoint[]>([]);
  const [sourceMetrics, setSourceMetrics] = useState<SourceMetricPoint[]>([]);
  const [, setParseErrorRows] = useState<ParseErrorSummary[]>([]);

  const [configuredDevices, setConfiguredDevices] = useState<FirewallDevice[]>([]);
  const [deviceModalOpen, setDeviceModalOpen] = useState(false);
  const [editingDevice, setEditingDevice] = useState<DeviceFormState>();
  const [customIpRegions, setCustomIpRegions] = useState<CustomIpRegion[]>([]);
  const [ipRegionModalOpen, setIpRegionModalOpen] = useState(false);
  const [editingIpRegion, setEditingIpRegion] = useState<CustomIpRegion>();

  const [archiveFiles, setArchiveFiles] = useState<ArchiveFile[]>([]);
  const [frozenFiles, setFrozenFiles] = useState<ArchiveFile[]>([]);
  const [archiveIndexRows, setArchiveIndexRows] = useState<ArchiveIndexRow[]>([]);
  const [archiveDays, setArchiveDays] = useState<string[]>([]);
  const [hotLogDays, setHotLogDays] = useState<string[]>([]);

  const [admissionCaseRows, setAdmissionCaseRows] = useState<AdmissionCase[]>([]);
  const [admissionProfileRows, setAdmissionProfileRows] = useState<DeviceProfile[]>([]);

  const [adaptiveRules, setAdaptiveRules] = useState<AdaptiveRule[]>([]);
  const [parserDiagnostics, setParserDiagnostics] = useState<ParserDiagnostic[]>([]);
  const [parserProfiles, setParserProfiles] = useState<ParserProfile[]>([]);
  const [parserScopes, setParserScopes] = useState<ParserScopeState[]>([]);

  const loadAdmission = async () => {
    const [nextCases, nextProfiles] = await Promise.all([admissionCases(), admissionProfiles()]);
    setAdmissionCaseRows(nextCases);
    setAdmissionProfileRows(nextProfiles);
  };

  const load = async () => {
    setLoading(true);
    try {
      const [
        nextStatus,
        nextEvents,
        nextMinuteMetrics,
        nextHourMetrics,
        nextSourceMetrics,
        nextParserSummary,
        nextArchiveFiles,
        nextFrozenFiles,
        nextArchiveIndex,
        nextArchiveDays,
        nextHotCalendarMetrics,
        nextDevices,
        nextCustomIpRegions,
        nextAdmissionCases,
        nextAdmissionProfiles,
        nextAdaptiveRules,
        nextParserDiagnostics,
        nextParserProfiles,
        nextParserScopes,
      ] = await Promise.all([
        fetchStatus().catch(() => undefined as SystemStatus | undefined),
        fetchEvents(500),
        fetchMinuteMetrics(24, 1440),
        fetchHourMetrics(24 * 365, 24 * 365),
        fetchSourceMetrics(24, 500),
        fetchParserSummary(),
        fetchArchiveFiles(),
        fetchFrozenFiles(),
        fetchArchiveIndex({ limit: '10000' }),
        fetchArchiveDays(),
        fetchMinuteMetrics(24 * 366, 10000),
        fetchDevices(),
        fetchCustomIpRegions(),
        admissionCases(),
        admissionProfiles(),
        fetchAdaptiveRules().then((r) => r.rules).catch(() => [] as AdaptiveRule[]),
        fetchParserDiagnostics().then((r) => r.diagnostics).catch(() => [] as ParserDiagnostic[]),
        fetchParserProfiles().then((r) => r.profiles).catch(() => [] as ParserProfile[]),
        fetchParserScopes().then((r) => r.scopes).catch(() => [] as ParserScopeState[]),
      ]);

      setSystemStatus(nextStatus);
      setEvents(nextEvents);
      setMinuteMetrics(nextMinuteMetrics);
      setHourMetrics(nextHourMetrics);
      setSourceMetrics(nextSourceMetrics);
      setParseErrorRows(nextParserSummary);
      setArchiveFiles(nextArchiveFiles);
      setFrozenFiles(nextFrozenFiles);
      setArchiveIndexRows(nextArchiveIndex);
      setArchiveDays(nextArchiveDays);
      setHotLogDays(
        Array.from(
          new Set(
            [
              ...nextEvents.map((event) => fmt(event.ingest_time).slice(0, 10)),
              ...nextHotCalendarMetrics.map((point) => fmt(point.bucket_minute).slice(0, 10)),
            ].filter((day) => /^\d{4}-\d{2}-\d{2}$/.test(day)),
          ),
        ),
      );
      setConfiguredDevices(nextDevices);
      setCustomIpRegions(nextCustomIpRegions);
      setAdmissionCaseRows(nextAdmissionCases);
      setAdmissionProfileRows(nextAdmissionProfiles);
      setAdaptiveRules(nextAdaptiveRules);
      setParserDiagnostics(nextParserDiagnostics);
      setParserProfiles(nextParserProfiles);
      setParserScopes(nextParserScopes);
    } catch (error) {
      message.error(error instanceof Error ? error.message : '刷新失败');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
    const timer = window.setInterval(load, 30000);
    return () => window.clearInterval(timer);
  }, []);

  const deviceBySourceKey = useMemo(() => {
    const map = new Map<string, FirewallDevice>();
    configuredDevices.forEach((device) => {
      deviceSourceKeys(device).forEach((key) => map.set(key, device));
    });
    return map;
  }, [configuredDevices]);

  const deviceRows = useMemo<DeviceRow[]>(() => {
    const map = new Map<string, DeviceRow>();
    const rowKeyBySourceKey = new Map<string, string>();
    configuredDevices.forEach((device) => {
      map.set(device.host, {
        id: device.id,
        name: device.name,
        device: device.host,
        sourceQuery: device.host,
        protocol: device.protocol,
        port: device.port,
        note: device.note,
        enabled: device.enabled,
        kind: 'managed',
        total: 0,
        parsed: 0,
        failed: 0,
        lastTime: '近24小时无日志',
      });
      deviceSourceKeys(device).forEach((key) => rowKeyBySourceKey.set(key, device.host));
    });

    sourceMetrics.forEach((metric) => {
      const device = deviceName(metric.source_addr);
      if (device === '-') return;
      const rowKey = rowKeyBySourceKey.get(device) || device;
      const current =
        map.get(rowKey) ||
        ({
          device,
          sourceQuery: metric.source_addr,
          protocol: deviceProtocol(metric.source_addr),
          kind: 'observed',
          total: 0,
          parsed: 0,
          failed: 0,
          lastTime: fmt(metric.last_seen) === '-' ? '近24小时有日志' : fmt(metric.last_seen),
        } satisfies DeviceRow);
      if (!current.sourceQuery || !metric.source_addr.includes(current.sourceQuery)) {
        current.sourceQuery = metric.source_addr;
      }
      current.total += metric.total;
      current.parsed += metric.parsed;
      current.failed += metric.failed;
      const lastSeen = fmt(metric.last_seen);
      if (lastSeen !== '-' && (current.lastTime === '近24小时无日志' || lastSeen > current.lastTime)) {
        current.lastTime = lastSeen;
      } else if (current.lastTime === '近24小时无日志') {
        current.lastTime = '近24小时有日志';
      }
      map.set(rowKey, current);
    });
    return Array.from(map.values()).sort((a, b) => b.total - a.total);
  }, [configuredDevices, sourceMetrics]);

  const displayDeviceName = (source?: string | null, raw?: string | null) => {
    const sourceName = deviceName(source, raw);
    const configured = deviceBySourceKey.get(sourceName);
    return configured?.name || sourceName;
  };

  const deviceFilterOptions = deviceRows.map((row) => ({
    label: row.name || row.device,
    value: row.sourceQuery || row.device,
  }));

  const assetRows = useMemo<AssetRow[]>(
    () => [
      ...archiveFiles.map((file) => ({ ...file, category: 'Parquet 冷库' as const })),
      ...frozenFiles.map((file) => ({ ...file, category: 'Frozen 原始归档' as const })),
    ],
    [archiveFiles, frozenFiles],
  );

  const archiveIndexTableRows = useMemo<ArchiveIndexTableRow[]>(
    () => archiveIndexRows.map((row) => ({ ...row, key: `${row.day}-${row.archive_path}` })),
    [archiveIndexRows],
  );

  const hotLogDaySet = useMemo(() => new Set(hotLogDays), [hotLogDays]);
  const coldLogDaySet = useMemo(
    () =>
      new Set(
        [
          ...archiveDays,
          ...archiveIndexRows.flatMap((row) => [
            fmt(row.day),
            ...archiveDaysFromText(row.archive_path),
            ...archiveDaysFromText(row.source_addr),
          ]),
          ...archiveFiles.flatMap((file) => archiveDaysFromText(file.path)),
          ...frozenFiles.flatMap((file) => archiveDaysFromText(file.path)),
        ].filter((day) => /^\d{4}-\d{2}-\d{2}$/.test(day)),
      ),
    [archiveDays, archiveFiles, archiveIndexRows, frozenFiles],
  );

  const navigate = (page: PageKey, filters?: Record<string, string>) => {
    if (filters) setLogFilters(filters);
    setActivePage(page);
  };

  const onBackfillDevices = async () => {
    try {
      setLoading(true);
      const result = await backfillDevices();
      message.success(`设备 ID 回填完成，更新 ${result.updated} 条日志`);
      await load();
    } catch (error) {
      message.error(error instanceof Error ? error.message : '设备 ID 回填失败');
    } finally {
      setLoading(false);
    }
  };

  const onRebuildArchiveIndex = async () => {
    try {
      setLoading(true);
      const result = await rebuildArchiveIndex();
      const rows = await fetchArchiveIndex({ limit: '1000' });
      setArchiveIndexRows(rows);
      message.success(`归档索引已重建，索引 ${result.indexed} 个文件`);
    } catch (error) {
      message.error(error instanceof Error ? error.message : '重建归档索引失败');
    } finally {
      setLoading(false);
    }
  };

  const onApproveAdmissionCase = async (row: AdmissionCase) => {
    const deviceId = window.prompt('输入设备 ID，例如 fw-sangfor-01', `device-${row.source_ip.replaceAll('.', '-')}`);
    if (!deviceId?.trim()) return;
    try {
      setLoading(true);
      await approveAdmissionCase(row.case_id, deviceId.trim(), 'web');
      await loadAdmission();
      message.success('设备指纹已批准');
    } catch (error) {
      message.error(error instanceof Error ? error.message : '批准准入案件失败');
    } finally {
      setLoading(false);
    }
  };

  const onBlockAdmissionCase = async (row: AdmissionCase) => {
    try {
      setLoading(true);
      await blockAdmissionCase(row.case_id);
      await loadAdmission();
      message.success('设备指纹已阻断');
    } catch (error) {
      message.error(error instanceof Error ? error.message : '阻断准入案件失败');
    } finally {
      setLoading(false);
    }
  };

  const onReopenAdmissionCase = async (row: AdmissionCase) => {
    try {
      setLoading(true);
      await reopenAdmissionCase(row.case_id);
      await loadAdmission();
      message.success('准入案件已重开');
    } catch (error) {
      message.error(error instanceof Error ? error.message : '重开准入案件失败');
    } finally {
      setLoading(false);
    }
  };

  const onCreateDevice = async (values: Record<string, string | number>) => {
    try {
      const input = {
        name: String(values.name || ''),
        host: String(values.host || ''),
        protocol: managedProtocol(String(values.protocol || 'UDP')),
        port: Number(values.port || 514),
        note: String(values.note || ''),
        enabled: editingDevice?.enabled ?? true,
      };
      const device = editingDevice?.id ? await updateDevice(editingDevice.id, input) : await createDevice(input);
      setConfiguredDevices((devices) =>
        editingDevice?.id ? devices.map((item) => (item.id === device.id ? device : item)) : [device, ...devices],
      );
      setDeviceModalOpen(false);
      setEditingDevice(undefined);
      message.success(editingDevice?.id ? '设备已更新' : '设备已纳管');
    } catch (error) {
      message.error(error instanceof Error ? error.message : '保存设备失败');
    }
  };

  const openDeviceModal = (row?: DeviceRow) => {
    if (row?.id) {
      setEditingDevice({
        id: row.id,
        name: row.name || '',
        host: row.device,
        protocol: managedProtocol(row.protocol),
        port: row.port || 514,
        note: row.note || '',
        enabled: row.enabled ?? true,
      });
    } else if (row) {
      setEditingDevice({
        name: row.name || row.device,
        host: row.device,
        protocol: managedProtocol(row.protocol),
        port: row.port || 514,
        note: row.note || '',
        enabled: true,
      });
    } else {
      setEditingDevice(undefined);
    }
    setDeviceModalOpen(true);
  };

  const onToggleDevice = async (row: DeviceRow) => {
    if (!row.id) return;
    try {
      const device = await updateDevice(row.id, {
        name: row.name || row.device,
        host: row.device,
        protocol: managedProtocol(row.protocol),
        port: row.port || 514,
        note: row.note || '',
        enabled: !row.enabled,
      });
      setConfiguredDevices((devices) => devices.map((item) => (item.id === device.id ? device : item)));
      message.success(device.enabled ? '设备已启用' : '设备已停用');
    } catch (error) {
      message.error(error instanceof Error ? error.message : '更新设备失败');
    }
  };

  const onDeleteDevice = async (row: DeviceRow) => {
    if (!row.id) return;
    Modal.confirm({
      title: '删除设备',
      content: `确认删除 ${row.name || row.device}？不会删除已入库日志。`,
      okText: '删除',
      okButtonProps: { danger: true },
      cancelText: '取消',
      onOk: async () => {
        if (!row.id) return;
        await deleteDevice(row.id);
        setConfiguredDevices((devices) => devices.filter((item) => item.id !== row.id));
        message.success('设备已删除');
      },
    });
  };

  const onCreateCustomIpRegion = async (values: Record<string, string>) => {
    try {
      const input = {
        cidr: String(values.cidr || ''),
        name: String(values.name || ''),
        note: String(values.note || ''),
        enabled: editingIpRegion?.enabled ?? true,
      };
      const rule = editingIpRegion
        ? await updateCustomIpRegion(editingIpRegion.id, input)
        : await createCustomIpRegion(input);
      setCustomIpRegions((rules) =>
        editingIpRegion
          ? rules.map((item) => (item.id === rule.id ? rule : item)).filter((item) => item.cidr !== rule.cidr || item.id === rule.id)
          : [rule, ...rules.filter((item) => item.cidr !== rule.cidr)],
      );
      setIpRegionModalOpen(false);
      setEditingIpRegion(undefined);
      message.success(editingIpRegion ? '自定义网段已更新' : '自定义网段已保存');
    } catch (error) {
      message.error(error instanceof Error ? error.message : '保存自定义网段失败');
    }
  };

  const onToggleCustomIpRegion = async (row: CustomIpRegion) => {
    try {
      const rule = await updateCustomIpRegion(row.id, {
        cidr: row.cidr,
        name: row.name,
        note: row.note || '',
        enabled: !row.enabled,
      });
      setCustomIpRegions((rules) => rules.map((item) => (item.id === rule.id ? rule : item)));
      message.success(rule.enabled ? '自定义网段已启用' : '自定义网段已停用');
    } catch (error) {
      message.error(error instanceof Error ? error.message : '更新自定义网段失败');
    }
  };

  const onDeleteCustomIpRegion = async (row: CustomIpRegion) => {
    Modal.confirm({
      title: '删除自定义 IP 归属地',
      content: `确认删除 ${row.cidr} / ${row.name}？`,
      okText: '删除',
      okButtonProps: { danger: true },
      cancelText: '取消',
      onOk: async () => {
        await deleteCustomIpRegion(row.id);
        setCustomIpRegions((rules) => rules.filter((item) => item.id !== row.id));
        message.success('自定义网段已删除');
      },
    });
  };

  const onEnableAdaptiveRule = async (ruleId: string) => {
    try {
      await enableAdaptiveRule(ruleId);
      setAdaptiveRules((prev) => prev.map((rule) => (rule.rule_id === ruleId ? { ...rule, status: 'active' } : rule)));
      message.success('规则已启用');
    } catch (error) {
      message.error(error instanceof Error ? error.message : '启用规则失败');
    }
  };

  const onDisableAdaptiveRule = async (ruleId: string) => {
    try {
      await disableAdaptiveRule(ruleId);
      setAdaptiveRules((prev) => prev.map((rule) => (rule.rule_id === ruleId ? { ...rule, status: 'disabled' } : rule)));
      message.success('规则已禁用');
    } catch (error) {
      message.error(error instanceof Error ? error.message : '禁用规则失败');
    }
  };

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
          onNavigate={navigate}
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
    if (activePage === 'sources') {
      return (
        <SourceGovernancePage
          loading={loading}
          deviceRows={deviceRows}
          customIpRegions={customIpRegions}
          admissionCaseRows={admissionCaseRows}
          admissionProfileRows={admissionProfileRows}
          onBackfillDevices={onBackfillDevices}
          onOpenDeviceModal={openDeviceModal}
          onToggleDevice={onToggleDevice}
          onDeleteDevice={onDeleteDevice}
          onOpenIpRegionModal={(row) => {
            setEditingIpRegion(row);
            setIpRegionModalOpen(true);
          }}
          onToggleCustomIpRegion={onToggleCustomIpRegion}
          onDeleteCustomIpRegion={onDeleteCustomIpRegion}
          onRefreshAdmission={loadAdmission}
          onApproveAdmissionCase={onApproveAdmissionCase}
          onBlockAdmissionCase={onBlockAdmissionCase}
          onReopenAdmissionCase={onReopenAdmissionCase}
          onViewLogs={(filters) => navigate('logs', filters)}
        />
      );
    }
    if (activePage === 'parser') {
      return (
        <ParserPage
          loading={loading}
          adaptiveRules={adaptiveRules}
          parserDiagnostics={parserDiagnostics}
          parserProfiles={parserProfiles}
          parserScopes={parserScopes}
          onEnableRule={onEnableAdaptiveRule}
          onDisableRule={onDisableAdaptiveRule}
        />
      );
    }
    return (
      <AssetsPage
        loading={loading}
        assetRows={assetRows}
        archiveIndexRows={archiveIndexTableRows}
        frozenCount={frozenFiles.length}
        totalBytes={assetRows.reduce((total, file) => total + file.bytes, 0)}
        onRebuildArchiveIndex={onRebuildArchiveIndex}
      />
    );
  };

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
      <Modal
        title={editingDevice?.id ? '编辑防火墙设备' : '纳管防火墙设备'}
        open={deviceModalOpen}
        onCancel={() => {
          setDeviceModalOpen(false);
          setEditingDevice(undefined);
        }}
        footer={null}
        destroyOnHidden
        width={560}
      >
        <Alert
          className="device-modal-alert"
          type="info"
          showIcon
          message="在防火墙上把 Syslog 目标地址指向本机，端口按这里配置。保存后设备会进入来源列表，日志到达后自动统计。"
        />
        <ProForm
          layout="vertical"
          submitter={{
            searchConfig: { submitText: editingDevice?.id ? '保存修改' : '纳管设备' },
            resetButtonProps: false,
          }}
          initialValues={
            editingDevice
              ? {
                  name: editingDevice.name,
                  host: editingDevice.host,
                  protocol: editingDevice.protocol,
                  port: editingDevice.port,
                  note: editingDevice.note,
                }
              : { protocol: 'UDP', port: 514 }
          }
          onFinish={async (values) => onCreateDevice(values as Record<string, string | number>)}
        >
          <ProFormText name="name" label="设备名称" placeholder="出口防火墙 / 核心防火墙" rules={[{ required: true, message: '请输入设备名称' }]} />
          <ProFormText name="host" label="管理地址" placeholder="192.168.0.1" rules={[{ required: true, message: '请输入设备管理地址' }]} />
          <ProFormSelect
            name="protocol"
            label="接入协议"
            options={[
              { label: 'UDP Syslog', value: 'UDP' },
              { label: 'TCP Syslog', value: 'TCP' },
              { label: 'TLS Syslog', value: 'TLS' },
            ]}
            rules={[{ required: true, message: '请选择接入协议' }]}
          />
          <ProFormDigit name="port" label="监听端口" min={1} max={65535} fieldProps={{ precision: 0 }} rules={[{ required: true, message: '请输入监听端口' }]} />
          <ProFormText name="note" label="备注" placeholder="机房、出口、线路或负责人" />
        </ProForm>
      </Modal>
      <Modal
        title={editingIpRegion ? '编辑自定义 IP 归属地' : '添加自定义 IP 归属地'}
        open={ipRegionModalOpen}
        onCancel={() => {
          setIpRegionModalOpen(false);
          setEditingIpRegion(undefined);
        }}
        footer={null}
        destroyOnHidden
        width={520}
      >
        <Alert
          className="device-modal-alert"
          type="info"
          showIcon
          message="自定义网段优先级高于 ip2region。命中后日志表和导出都会显示这里配置的归属地名称。"
        />
        <ProForm
          layout="vertical"
          submitter={{
            searchConfig: { submitText: editingIpRegion ? '保存修改' : '保存网段' },
            resetButtonProps: false,
          }}
          initialValues={
            editingIpRegion
              ? { cidr: editingIpRegion.cidr, name: editingIpRegion.name, note: editingIpRegion.note }
              : undefined
          }
          onFinish={async (values) => onCreateCustomIpRegion(values as Record<string, string>)}
        >
          <ProFormText name="cidr" label="CIDR 网段" placeholder="203.0.113.0/24" rules={[{ required: true, message: '请输入 CIDR 网段' }]} />
          <ProFormText name="name" label="归属地名称" placeholder="外联业务网段 / 专线出口" rules={[{ required: true, message: '请输入归属地名称' }]} />
          <ProFormText name="note" label="备注" placeholder="用途、负责人或线路说明" />
        </ProForm>
      </Modal>
    </OxideLogShell>
  );
};

export default OxideLogPage;
