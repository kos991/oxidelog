import {
  CloudServerOutlined,
  DashboardOutlined,
  DatabaseOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  ReloadOutlined,
  SearchOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons';
import { Button, Layout, Menu, Space, Tooltip, Typography } from 'antd';
import React from 'react';
import type { PageKey } from '../types';

const { Header, Sider, Content } = Layout;
const { Text, Title } = Typography;

const navItems = [
  { key: 'overview', icon: <DashboardOutlined />, label: '总览' },
  { key: 'logs', icon: <SearchOutlined />, label: '日志工作台' },
  { key: 'sources', icon: <CloudServerOutlined />, label: '来源治理' },
  { key: 'parser', icon: <ThunderboltOutlined />, label: '解析器管理' },
  { key: 'assets', icon: <DatabaseOutlined />, label: '归档资产' },
] satisfies { key: PageKey; icon: React.ReactNode; label: string }[];

const pageCopy: Record<PageKey, { title: string; subtitle: string }> = {
  overview: { title: '总览', subtitle: '日志接入、解析健康、来源风险与存储状态' },
  logs: { title: '日志工作台', subtitle: '查询、筛选、结果研判与导出任务' },
  sources: { title: '来源治理', subtitle: '设备来源、观察来源、准入审批与信任/阻断历史' },
  parser: { title: '解析器管理', subtitle: '自学习规则、诊断、Profile 与 Scope 状态' },
  assets: { title: '归档资产', subtitle: 'Parquet 冷库、Frozen 原始归档与索引状态' },
};

const AnimatedLogo = () => (
  <svg className="brand-logo-svg" viewBox="0 0 48 48" aria-hidden="true">
    <defs>
      <linearGradient id="oxideLogoGradient" x1="0" x2="1" y1="0" y2="1">
        <stop offset="0%" stopColor="#1677ff" />
        <stop offset="100%" stopColor="#13c2c2" />
      </linearGradient>
    </defs>
    <circle className="logo-orbit" cx="24" cy="24" r="17" fill="none" stroke="url(#oxideLogoGradient)" strokeWidth="4" />
    <path className="logo-pulse" d="M15 25h7l3-8 4 15 3-7h4" fill="none" stroke="#fff" strokeLinecap="round" strokeLinejoin="round" strokeWidth="3" />
    <circle className="logo-core" cx="24" cy="24" r="5" fill="#fff" />
  </svg>
);

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
      <Sider className="app-sider" width={232} breakpoint="lg" collapsed={collapsed} collapsedWidth={72} trigger={null}>
        <div className="brand">
          <Tooltip title={collapsed ? 'OxideLog' : undefined} placement="right">
            <div className="brand-mark">
              <AnimatedLogo />
            </div>
          </Tooltip>
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
          <span className={workerErrors && workerErrors > 0 ? 'health-dot warning' : 'health-dot'} />
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
