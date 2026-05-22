export default [
  {
    path: '/',
    redirect: '/oxidelog',
  },
  {
    path: '/app',
    redirect: '/oxidelog',
  },
  {
    path: '/oxidelog',
    name: 'OxideLog',
    icon: 'dashboard',
    layout: false,
    component: './oxidelog',
  },
  {
    path: '/*',
    layout: false,
    redirect: '/oxidelog',
  },
];
