{
  clientGrpcServers: [{
    listenAddresses: [':8982'],
    authenticationPolicy: { allow: {} },
  }],
  workerGrpcServers: [{
    listenAddresses: [':8983'],
    authenticationPolicy: { allow: {} },
  }],
  buildQueueStateGrpcServers: [{
    listenAddresses: [':8984'],
    authenticationPolicy: { allow: {} },
  }],
  contentAddressableStorage: { grpc: { client: { address: 'bb-storage:8980' } } },
  maximumMessageSizeBytes: 2 * 1024 * 1024,
  executeAuthorizer: { allow: {} },
  modifyDrainsAuthorizer: { allow: {} },
  killOperationsAuthorizer: { allow: {} },
  synchronizeAuthorizer: { allow: {} },
  actionRouter: {
    simple: {
      platformKeyExtractor: { action: {} },
      invocationKeyExtractors: [
        { correlatedInvocationsId: {} },
        { toolInvocationId: {} },
      ],
      initialSizeClassAnalyzer: {
        defaultExecutionTimeout: '1800s',
        maximumExecutionTimeout: '7200s',
      },
    },
  },
  platformQueueWithNoWorkersTimeout: '900s',
  // Otherwise an execution that comes before the worker's first synchronization fails
  // instead of waiting in the queue.
  predeclaredPlatformQueues: [{
    instanceNamePrefix: '',
    platform: { properties: [{ name: 'OSFamily', value: 'linux' }] },
    // The worker's, which sets no size class.
    sizeClasses: [0],
  }],
}
