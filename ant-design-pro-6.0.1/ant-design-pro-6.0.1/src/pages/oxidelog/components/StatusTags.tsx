import React from 'react';
import { Tag } from 'antd';
import type { AdaptiveRuleStatus, CanonicalEvent } from '@/services/oxidelog';
import { statusLabel } from '../utils';

export const ParseStatusTag: React.FC<{ status?: CanonicalEvent['parse_status'] }> = ({ status }) => {
  if (status === 'parsed') return <Tag color="success">{statusLabel(status)}</Tag>;
  if (status === 'partial') return <Tag color="warning">{statusLabel(status)}</Tag>;
  return <Tag color="error">{statusLabel(status)}</Tag>;
};

export const AdmissionStateTag: React.FC<{ state?: string }> = ({ state }) => {
  if (state === 'trusted') return <Tag color="success">已信任</Tag>;
  if (state === 'blocked') return <Tag color="error">已阻断</Tag>;
  return <Tag color="processing">待审批</Tag>;
};

export const DeviceStateTag: React.FC<{ managed?: boolean; enabled?: boolean }> = ({ managed, enabled }) => {
  if (!managed) return <Tag>观察来源</Tag>;
  return enabled ? <Tag color="success">启用</Tag> : <Tag>停用</Tag>;
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
