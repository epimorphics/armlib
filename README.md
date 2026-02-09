# Asynchronous Request Management library (armlib)

Support for batch APIs as a complement to normal sapi interactive APIs.

Provides a (distributable) queue for pending requests, a status manager to determine the status of requests and caching for completed requests. The cache is split into persistent and temporary areas (through use of a `sticky` flag). Typically ad hoc requests are treated as temporary and results are cleared out overnight; whereas, predefined downloads are kept in persistent cache and only cleared out on data rebuilds.

`armlib` only provides the queue and cache management. It is up to the calling application to construct the requests and to execute them when they reach the head of the queue. Typically both request construction and query processing are carried out by a SAPI application and arranged so that identical query forms are available in both interactive and batch mode..

The implementation of the QueueManager and CacheManager is pluggable using `appbase` configuration machinery. Two pairs of implementation are provided.

For development use, where only one server is involved, use caching in local files (`FileCacheManager`) and in-memory queues (`MemQueueManager`).

For production use any server may receive queue requests and any server may process requests off the queue. To support this a distributed queue system is provided using AWS Dynamo DB (`DynQueueManager`) and caching is distributed via AWS S3 (`S3CacheManager`). Using Dynamo is negligible cost and means that the state of the queue can be inspected (and, to some extent, manipulated) via the AWS UI.

## Use

The calling application should configure the use of the `armlib` services and plug in the appropriate queue and cache implementation using an `app.conf` file. This is normally done by instantiating a `StandardRequestManager` though it is possible to plug-in an alternative implementation of the `RequestManager` interface.

The calling application should construct instances of `BatchRequest` to represent a request to queue. Batch requests are essentially formatted as web requests with an option `sticky` flag to indicate the preferred caching lifetime. These requests should then be submitted to `armlib` using the `RequestManager` interface, which also supports status tracking and cache access.
