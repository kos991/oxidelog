export default {
  dev: {
    '/api/': {
      target: 'http://127.0.0.1:18080',
      changeOrigin: true,
    },
  },
  test: {
    '/api/': {
      target: 'https://pro-api.ant-design-demo.workers.dev',
      changeOrigin: true,
    },
  },
  pre: {
    '/api/': {
      target: 'your pre url',
      changeOrigin: true,
    },
  },
};
