import type { ProLayoutProps } from '@ant-design/pro-components';

const Settings: ProLayoutProps & {
  logo?: string;
} = {
  navTheme: 'light',
  colorPrimary: '#1677ff',
  layout: 'top',
  contentWidth: 'Fluid',
  fixedHeader: true,
  fixSiderbar: true,
  splitMenus: false,
  colorWeak: false,
  title: 'OxideLog',
  logo: false,
  token: {
    header: {
      colorBgHeader: '#ffffff',
      colorHeaderTitle: '#172033',
      colorTextMenu: '#42526a',
      colorTextMenuSelected: '#1677ff',
    },
    sider: {
      colorMenuBackground: '#ffffff',
      colorTextMenu: '#42526a',
      colorTextMenuSelected: '#1677ff',
      colorBgMenuItemSelected: '#eef5ff',
    },
  },
};

export default Settings;
