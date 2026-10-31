// A worker that hardlinks inputs into its build directory. Unlike the FUSE worker of
// bb-deployments, it needs no privileges. It runs actions through bb-runner, which shares
// /worker.
local storage = { grpc: { client: { address: 'bb-storage:8980' } } };

{
  blobstore: {
    contentAddressableStorage: storage,
    actionCache: storage,
  },
  portalUrl: 'http://localhost/browser',
  maximumMessageSizeBytes: 2 * 1024 * 1024,
  scheduler: { address: 'bb-scheduler:8983' },
  buildDirectories: [{
    native: {
      buildDirectoryPath: '/worker/build',
      cacheDirectoryPath: '/worker/cache',
      maximumCacheFileCount: 10000,
      maximumCacheSizeBytes: 4 * 1024 * 1024 * 1024,
      cacheReplacementPolicy: 'LEAST_RECENTLY_USED',
    },
    runners: [{
      endpoint: { address: 'unix:///worker/runner' },
      concurrency: 4,
      instanceNamePrefix: '',
      // test-reapi-buildbarn sends the same property with every action.
      platform: {
        properties: [{ name: 'OSFamily', value: 'linux' }],
      },
      workerId: { hostname: 'bb-worker' },
    }],
  }],
  inputDownloadConcurrency: 10,
  outputUploadConcurrency: 11,
  directoryCache: {
    maximumCount: 1000,
    maximumSizeBytes: 1000 * 1024,
    cacheReplacementPolicy: 'LEAST_RECENTLY_USED',
  },
}
