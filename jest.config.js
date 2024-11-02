module.exports = {
  transform: {
    '^.+\\.ts$': [ 'ts-jest', {
      isolatedModules: true,
    }],
  },
  testRegex: '/test/.*-test.ts$',
  moduleFileExtensions: [
    'ts',
    'js',
  ],
  globals: {
    window: {
      location: new URL('http://localhost'),
    },
  },
  setupFilesAfterEnv: [ './setup-jest.js' ],
  collectCoverage: true,
  coveragePathIgnorePatterns: [
    '/node_modules/',
    '/mocks/',
    '/dev-tools/',
    'engine-default.js',
    'util.ts',
    'index.js',
  ],
  testEnvironment: 'node',
  coverageThreshold: {
    global: {
      branches: 100,
      functions: 100,
      lines: 100,
      statements: 100,
    },
  },
};
