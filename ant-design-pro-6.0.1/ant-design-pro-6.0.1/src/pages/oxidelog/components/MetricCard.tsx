import { ProCard } from '@ant-design/pro-components';
import { Typography } from 'antd';
import React from 'react';

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
