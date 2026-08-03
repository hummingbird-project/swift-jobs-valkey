//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

public import Jobs
public import Logging
public import NIOCore
public import Synchronization
public import Valkey

#if canImport(FoundationEssentials)
public import FoundationEssentials
#else
public import Foundation
#endif

/// Valkey implementation of job queue driver
public final class ValkeyJobQueue: JobQueueDriver {
    /// Identifier for a job instance
    public struct JobID: Sendable, CustomStringConvertible, Equatable, Codable, RESPStringRenderable, RESPTokenDecodable {
        @usableFromInline
        let value: UUID

        @usableFromInline
        init() {
            self.value = UUID()
        }

        init?(uuidString: String) {
            guard let value = UUID(uuidString: uuidString) else { return nil }
            self.value = value
        }

        public init(_ token: RESPToken) throws(RESPDecodeError) {
            let string = try token.decode(as: String.self)
            guard let uuid = UUID(uuidString: string) else { throw RESPDecodeError(.unexpectedToken, token: token) }
            self.value = uuid
        }

        public var respEntries: Int { 1 }

        public func encode(into commandEncoder: inout ValkeyCommandEncoder) {
            self.value.uuidString.encode(into: &commandEncoder)
        }

        /// String description of Identifier
        public var description: String {
            self.value.uuidString
        }

        public init(from decoder: any Decoder) throws {
            let container = try decoder.singleValueContainer()
            self.value = try container.decode(UUID.self)
        }

        public func encode(to encoder: any Encoder) throws {
            var container = encoder.singleValueContainer()
            try container.encode(value)
        }
    }

    /// Options for job pushed to queue
    public struct JobOptions: JobOptionsProtocol {
        /// Delay running job until
        public var delayUntil: Date

        /// Default initializer for JobOptions
        public init() {
            self.delayUntil = .now
        }

        /// Initializer for JobOptions
        /// - Parameter delayUntil: Time to delay job until
        public init(delayUntil: Date) {
            self.delayUntil = delayUntil
        }
    }

    public struct ValkeyQueueError: Error, CustomStringConvertible {
        enum Internal {
            case unexpectedValkeyResponse
        }
        let value: Internal

        /// Valkey command returned an unexpected token
        public static var unexpectedValkeyResponse: Self { .init(value: .unexpectedValkeyResponse) }

        public var description: String {
            switch self.value {
            case .unexpectedValkeyResponse:
                return "Unexpected Valkey Response"
            }
        }
    }

    @usableFromInline
    enum Status: String, Sendable {
        case pending
        case processing
        case failed
        case cancelled
        case paused
        case completed
    }

    /// metadata keys
    @usableFromInline
    static let statusKey = "status"
    static let workerIDMetaDataKey = "workerID"
    static let processingStartedMetaDataKey = "processingStarted"

    @usableFromInline
    let valkeyClient: ValkeyClient
    @usableFromInline
    let configuration: Configuration
    @usableFromInline
    let isStopped: Atomic<Bool>
    @usableFromInline
    let logger: Logger
    public let context: JobQueueContext

    let loadFunctions: AsyncInitializedGlobal<Void>

    /// Initialize Valkey job queue
    /// - Parameters:
    ///   - valkeyClient: Valkey client
    ///   - configuration: configuration
    ///   - logger: Logger used by ValkeyJobQueue
    public init(_ valkeyClient: ValkeyClient, configuration: Configuration = .init(), logger: Logger) async throws {
        self.valkeyClient = valkeyClient
        self.configuration = configuration
        self.isStopped = .init(false)
        self.jobRegistry = .init()
        self.logger = logger
        self.loadFunctions = .init()
        self.context = JobQueueContext(workerID: UUID().uuidString, queueName: configuration.queueName, metadata: [:])
        self.registerCleanupJob()
    }

    /// Initialize loading of functions and wait until it has finished
    public func waitUntilReady() async throws {
        try await self.loadFunctions()
        try await self.cleanupOrphanedJobs(maxJobsToProcess: .max)
    }

    /// Return the Valkey key for a job
    @inlinable
    public func valkeyKey(forJobID jobID: JobID) -> ValkeyKey {
        .init("\(self.configuration.queueName)/\(jobID.value)")
    }

    /// Return the Valkey key for the metadata of a job
    @inlinable
    public func valkeyMetadataKey(forJobID jobID: JobID) -> ValkeyKey {
        .init("\(self.configuration.queueName)/\(jobID.value).metadata")
    }

    ///  Register job
    /// - Parameters:
    ///   - job: Job Definition
    public func registerJob<Parameters>(_ job: JobDefinition<Parameters>) {
        self.jobRegistry.registerJob(job)
    }

    /// Push job data onto queue
    /// - Parameters:
    ///   - jobRequest: Job request
    ///   - options: Job options
    /// - Returns: Job ID
    @discardableResult
    @inlinable
    public func push<Parameters>(_ jobRequest: JobRequest<Parameters>, options: JobOptions) async throws -> JobID {
        let jobInstanceID = JobID()
        try await self.push(jobID: jobInstanceID, jobRequest: jobRequest, options: options)
        return jobInstanceID
    }

    /// Retry job data onto queue
    /// - Parameters:
    ///   - id: Job instance ID
    ///   - jobRequest: Job request
    ///   - options: Job retry options
    @inlinable
    public func retry<Parameters>(_ id: JobID, jobRequest: JobRequest<Parameters>, options: JobRetryOptions) async throws {
        let options = JobOptions(delayUntil: options.delayUntil)
        let buffer = try self.jobRegistry.encode(jobRequest: jobRequest)
        _ = try await self.valkeyClient.execute(
            LREM(self.configuration.processingQueueKey, count: 0, element: id),
            DEL(keys: [self.valkeyMetadataKey(forJobID: id)]),
            SET(self.valkeyKey(forJobID: id), value: buffer),
            HSET(
                self.valkeyMetadataKey(forJobID: id),
                data: [.init(field: Self.statusKey, value: Status.pending.rawValue)]
            ),
            ZADD(
                self.configuration.pendingQueueKey,
                data: [
                    .init(
                        score: options.delayUntil.timeIntervalSince1970,
                        member: id.description
                    )
                ]
            )
        ).3.get()
    }

    /// Helper for enqueuing jobs
    @usableFromInline
    func push<Parameters>(jobID: JobID, jobRequest: JobRequest<Parameters>, options: JobOptions) async throws {
        let buffer = try self.jobRegistry.encode(jobRequest: jobRequest)
        _ = try await valkeyClient.execute(
            SET(self.valkeyKey(forJobID: jobID), value: buffer),
            ZADD(
                self.configuration.pendingQueueKey,
                data: [
                    .init(
                        score: options.delayUntil.timeIntervalSince1970,
                        member: jobID.description
                    )
                ]
            ),
            HSET(
                jobID.valkeyMetadataKey(for: self),
                data: [.init(field: Self.statusKey, value: Status.pending.rawValue)]
            )
        ).1.get()
    }

    /// Flag job is done
    ///
    /// Removes  job id from processing queue
    /// - Parameters:
    ///   - jobID: Job id
    @inlinable
    public func finished(jobID: JobID) async throws {
        if self.configuration.retentionPolicy.completedJobs == .retain {
            _ = try await self.valkeyClient.execute(
                LREM(self.configuration.processingQueueKey, count: 0, element: jobID),
                ZADD(self.configuration.completedQueueKey, data: [.init(score: Date.now.timeIntervalSince1970, member: jobID)]),
                HSET(
                    jobID.valkeyMetadataKey(for: self),
                    data: [.init(field: Self.statusKey, value: Status.completed.rawValue)]
                )
            ).1.get()
        } else {
            _ = try await self.valkeyClient.execute(
                LREM(self.configuration.processingQueueKey, count: 0, element: jobID),
                DEL(keys: [self.valkeyKey(forJobID: jobID), self.valkeyMetadataKey(forJobID: jobID)])
            ).1.get()
        }
    }

    /// Flag job failed to process
    ///
    /// Removes  job id from processing queue, adds to failed queue
    /// - Parameters:
    ///   - jobID: Job id
    @inlinable
    public func failed(jobID: JobID, error: any Error) async throws {
        if self.configuration.retentionPolicy.failedJobs == .retain {
            _ = try await self.valkeyClient.execute(
                LREM(self.configuration.processingQueueKey, count: 0, element: jobID),
                ZADD(self.configuration.failedQueueKey, data: [.init(score: Date.now.timeIntervalSince1970, member: jobID)]),
                HSET(
                    self.valkeyMetadataKey(forJobID: jobID),
                    data: [.init(field: Self.statusKey, value: Status.failed.rawValue)]
                )
            ).1.get()
        } else {
            _ = try await self.valkeyClient.execute(
                LREM(self.configuration.processingQueueKey, count: 0, element: jobID),
                DEL(keys: [self.valkeyKey(forJobID: jobID), self.valkeyMetadataKey(forJobID: jobID)])
            ).1.get()
        }
    }

    public func stop() async {
        self.isStopped.store(true, ordering: .relaxed)
    }

    public func shutdownGracefully() async {}

    /// Pop Job off queue and add to pending queue
    /// - Parameter eventLoop: eventLoop to do work on
    /// - Returns: queued job
    @usableFromInline
    func popFirst() async throws -> JobQueueResult<JobID>? {
        let value = try await self.valkeyClient.fcall(
            function: "swiftjobs_pop",
            keys: [self.configuration.pendingQueueKey, self.configuration.processingQueueKey],
            args: ["\(Date.now.timeIntervalSince1970)"]
        )
        guard let jobID = try? value.decode(as: JobID.self) else {
            return nil
        }

        let results = await self.valkeyClient.execute(
            GET(self.valkeyKey(forJobID: jobID)),
            HSET(
                self.valkeyMetadataKey(forJobID: jobID),
                data: [
                    .init(field: Self.workerIDMetaDataKey, value: self.context.workerID),
                    .init(field: Self.processingStartedMetaDataKey, value: "\(Date.now.timeIntervalSince1970)"),
                    .init(field: Self.statusKey, value: Status.processing.rawValue),
                ]
            )
        )
        if let buffer = try results.0.get().map({ ByteBuffer($0) }) {
            do {
                let jobInstance = try self.jobRegistry.decode(buffer)
                return .init(id: jobID, result: .success(jobInstance))
            } catch let error as JobQueueError {
                return .init(id: jobID, result: .failure(error))
            }
        } else {
            return .init(id: jobID, result: .failure(JobQueueError(code: .unrecognisedJobId, jobName: nil)))
        }
    }

    func get(jobID: JobID) async throws -> ByteBuffer? {
        try await self.valkeyClient.get(self.valkeyKey(forJobID: jobID)).map { ByteBuffer($0) }
    }

    func delete(jobIDs: [JobID]) async throws {
        _ = try await self.valkeyClient.del(keys: jobIDs.flatMap { [self.valkeyKey(forJobID: $0), self.valkeyMetadataKey(forJobID: $0)] })
    }

    @usableFromInline
    let jobRegistry: JobRegistry
}

extension ValkeyJobQueue: JobMetadataDriver {
    /// Get job queue metadata
    /// - Parameter key: Metadata key
    /// - Returns: Associated ByteBuffer
    @inlinable
    public func getMetadata(_ key: String) async throws -> ByteBuffer? {
        let key = "\(self.configuration.metadataKeyPrefix)\(key)"
        return try await self.valkeyClient.get(.init(key)).map { ByteBuffer($0) }
    }

    /// Set job queue metadata
    /// - Parameters:
    ///   - key: Metadata key
    ///   - value: Associated ByteBuffer
    @inlinable
    public func setMetadata(key: String, value: ByteBuffer) async throws {
        let key = "\(self.configuration.metadataKeyPrefix)\(key)"
        try await self.valkeyClient.set(.init(key), value: value)
    }

    /// Acquire metadata lock
    ///
    /// - Parameters:
    ///   - key: Metadata key
    ///   - id: Lock identifier
    ///   - expiresIn: When lock will expire
    /// - Returns: If lock was acquired
    @inlinable
    public func acquireLock(key: String, id: ByteBuffer, expiresIn: TimeInterval) async throws -> Bool {
        let key = ValkeyKey("\(self.configuration.metadataKeyPrefix)\(key)")
        return try await self.valkeyClient.withConnection { connection in
            try await connection.watch(keys: [key])
            let contents = try await connection.get(key).map { ByteBuffer($0) }
            if contents == id {
                try await connection.expireat(key, unixTimeSeconds: Date.now + expiresIn)
                return true
            } else {
                return try await connection.set(key, value: id, condition: .nx, expiration: .unixTimeSeconds(Date.now + expiresIn)) != nil
            }
        }
    }

    /// Release metadata lock
    ///
    /// - Parameters:
    ///   - key: Metadata key
    ///   - id: Lock identifier
    @inlinable
    public func releaseLock(key: String, id: ByteBuffer) async throws {
        let key = ValkeyKey("\(self.configuration.metadataKeyPrefix)\(key)")
        try await self.valkeyClient.withConnection { connection in
            let contents = try await connection.get(key).map { ByteBuffer($0) }
            if contents == id {
                try await connection.del(keys: [key])
            }
        }
    }
}

/// extend ValkeyJobQueue to conform to AsyncSequence
extension ValkeyJobQueue {
    public typealias Element = JobQueueResult<JobID>
    public struct AsyncIterator: AsyncIteratorProtocol {
        @usableFromInline
        let queue: ValkeyJobQueue

        @inlinable
        mutating public func next() async throws -> Element? {
            while true {
                if self.queue.isStopped.load(ordering: .relaxed) {
                    return nil
                }
                if let job = try await queue.popFirst() {
                    return job
                }
                // we only sleep if we didn't receive a job
                try await Task.sleep(for: self.queue.configuration.pollTime)
            }
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        .init(queue: self)
    }
}

extension ValkeyJobQueue: CancellableJobQueue {
    /// Cancels a job
    ///
    /// Removes it from the pending queue
    /// - Parameters:
    ///  - jobID: Job id
    @inlinable
    public func cancel(jobID: JobID) async throws {
        if self.configuration.retentionPolicy.cancelledJobs == .retain {
            _ = try await self.valkeyClient.fcall(
                function: "swiftjobs_cancelAndRetain",
                keys: [self.configuration.pendingQueueKey, self.configuration.cancelledQueueKey, jobID.valkeyMetadataKey(for: self)],
                args: [jobID.description, "\(Date.now.timeIntervalSince1970)", Status.cancelled.rawValue]
            )
        } else {
            _ = try await self.valkeyClient.execute(
                ZREM(self.configuration.pendingQueueKey, members: [jobID]),
                DEL(keys: [self.valkeyKey(forJobID: jobID), self.valkeyMetadataKey(forJobID: jobID)])
            ).1.get()
        }
    }
}

extension ValkeyJobQueue: ResumableJobQueue {
    /// Temporarily remove job from pending queue
    ///
    /// Removes it from the pending queue, adds to paused queue
    /// - Parameters:
    ///  - jobID: Job id
    @inlinable
    public func pause(jobID: JobID) async throws {
        _ = try await self.valkeyClient.fcall(
            function: "swiftjobs_pauseResume",
            keys: [self.configuration.pendingQueueKey, self.configuration.pausedQueueKey, jobID.valkeyMetadataKey(for: self)],
            args: [jobID.description, Status.paused.rawValue]
        )
    }

    /// Moved paused job back onto pending queue
    ///
    /// Removes it from the paused queue, adds to pending queue
    /// - Parameters:
    ///  - jobID: Job id
    @inlinable
    public func resume(jobID: JobID) async throws {
        _ = try await self.valkeyClient.fcall(
            function: "swiftjobs_pauseResume",
            keys: [self.configuration.pausedQueueKey, self.configuration.pendingQueueKey, jobID.valkeyMetadataKey(for: self)],
            args: [jobID.description, Status.pending.rawValue]
        )
    }
}

extension JobQueueDriver where Self == ValkeyJobQueue {
    /// Return Valkey driver for Job Queue
    /// - Parameters:
    ///   - valkeyClient: Valkey client
    ///   - configuration: configuration
    ///   - logger: Logger used by ValkeyJobQueue
    public static func valkey(
        _ valkeyClient: ValkeyClient,
        configuration: ValkeyJobQueue.Configuration = .init(),
        logger: Logger
    ) async throws -> Self {
        try await .init(valkeyClient, configuration: configuration, logger: logger)
    }
}
