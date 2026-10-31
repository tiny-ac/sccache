// One bb_storage that keeps the CAS and the action cache, and forwards Execute requests to
// bb-scheduler, as the frontend and the storage of bb-deployments do together.
//
// The action cache needs a single new block, since its entries are replaced. A blob must fit
// in one block, and the files are sparse.
local blobstore(dir, keysBytes, blocksBytes, newBlocks) = {
  'local': {
    keyLocationMapOnBlockDevice: {
      file: { path: dir + '/key_location_map', sizeBytes: keysBytes },
    },
    keyLocationMapMaximumGetAttempts: 16,
    keyLocationMapMaximumPutAttempts: 64,
    oldBlocks: 8,
    currentBlocks: 24,
    newBlocks: newBlocks,
    blocksOnBlockDevice: {
      source: { file: { path: dir + '/blocks', sizeBytes: blocksBytes } },
      spareBlocks: 3,
    },
  },
};

{
  grpcServers: [{
    listenAddresses: [':8980'],
    authenticationPolicy: { allow: {} },
  }],
  schedulers: {
    '': { endpoint: { address: 'bb-scheduler:8982' } },
  },
  maximumMessageSizeBytes: 2 * 1024 * 1024,
  contentAddressableStorage: {
    // 38 blocks of about 860 MiB, so that librustc_driver fits in one.
    backend: blobstore('/storage/cas', 64 * 1024 * 1024, 32 * 1024 * 1024 * 1024, 3),
    getAuthorizer: { allow: {} },
    putAuthorizer: { allow: {} },
    findMissingAuthorizer: { allow: {} },
  },
  actionCache: {
    backend: blobstore('/storage/ac', 1024 * 1024, 20 * 1024 * 1024, 1),
    getAuthorizer: { allow: {} },
    putAuthorizer: { allow: {} },
  },
  executeAuthorizer: { allow: {} },
}
