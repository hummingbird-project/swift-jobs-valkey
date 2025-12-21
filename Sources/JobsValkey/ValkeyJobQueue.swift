//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2021-2021 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Jobs
import Logging
import NIOCore
import Synchronization
import Valkey

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Valkey implementation of job queue driver
public final class ValkeyJobQueue: JobQueueDriver {
    public struct JobID: Sendable, CustomStringConvertible, Equatable, RESPStringRenderable, RESPTokenDecodable {
        let value: String

        @usableFromInline
        init() {
            self.value = UUID().uuidString
        }

        init(value: String) {
            self.value = value
        }

        init(buffer: ByteBuffer) {
            self.value = String(buffer: buffer)
        }

        public init(_ token: RESPToken) throws {
            self.value = try String(token)
        }

        public var respEntries: Int { 1 }

        public func encode(into commandEncoder: inout ValkeyCommandEncoder) {
            self.value.encode(into: &commandEncoder)
        }

        @usableFromInline
        func valkeyKey(for queue: ValkeyJobQueue) -> ValkeyKey { .init("\(queue.configuration.queueName)/\(self.description)") }

        /// String description of Identifier
        public var description: String {
            self.value
        }
    }

    /// Options for job pushed to queue
    public struct JobOptions: JobOptionsProtocol {
        /// Delay running job until
        public var delayUntil: Date

        /// Default initializer for JobOptions
        public init(delayUntil: Date) {
            self.delayUntil = delayUntil
        }
    }

    public enum ValkeyQueueError: Error, CustomStringConvertible {
        case unexpectedValkeyKeyType
        case jobMissing(JobID)

        public var description: String {
            switch self {
            case .unexpectedValkeyKeyType:
                return "Unexpected Valkey key type"
            case .jobMissing(let value):
                return "Job associated with \(value) is missing"
            }
        }
    }

    @usableFromInline
    let valkeyClient: ValkeyClient
    @usableFromInline
    let configuration: Configuration
    @usableFromInline
    let isStopped: Atomic<Bool>
    @usableFromInline
    let logger: Logger

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
        self.registerCleanupJob()
    }

    /// Initialize loading of functions and wait until it has finished
    public func waitUntilReady() async throws {
        try await self.loadFunctions()
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
        try await self.finished(jobID: id)
        try await self.push(jobID: id, jobRequest: jobRequest, options: options)
    }

    /// Helper for enqueuing jobs
    @usableFromInline
    func push<Parameters>(jobID: JobID, jobRequest: JobRequest<Parameters>, options: JobOptions) async throws {
        let buffer = try self.jobRegistry.encode(jobRequest: jobRequest)
        _ = try await valkeyClient.execute(
            SET(jobID.valkeyKey(for: self), value: buffer),
            ZADD(
                self.configuration.pendingQueueKey,
                data: [
                    .init(
                        score: options.delayUntil.timeIntervalSince1970,
                        member: jobID.description
                    )
                ]
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
                ZADD(self.configuration.completedQueueKey, data: [.init(score: Date.now.timeIntervalSince1970, member: jobID)])
            ).1.get()
        } else {
            _ = try await self.valkeyClient.execute(
                LREM(self.configuration.processingQueueKey, count: 0, element: jobID),
                DEL(keys: [jobID.valkeyKey(for: self)])
            ).1.get()
        }
    }

    /// Flag job failed to process
    ///
    /// Removes  job id from processing queue, adds to failed queue
    /// - Parameters:
    ///   - jobID: Job id
    @inlinable
    public func failed(jobID: JobID, error: Error) async throws {
        if self.configuration.retentionPolicy.failedJobs == .retain {
            _ = try await self.valkeyClient.execute(
                LREM(self.configuration.processingQueueKey, count: 0, element: jobID),
                ZADD(self.configuration.failedQueueKey, data: [.init(score: Date.now.timeIntervalSince1970, member: jobID)])
            ).1.get()
        } else {
            _ = try await self.valkeyClient.execute(
                LREM(self.configuration.processingQueueKey, count: 0, element: jobID),
                DEL(keys: [jobID.valkeyKey(for: self)])
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

        if let buffer = try await self.get(jobID: jobID) {
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
        try await self.valkeyClient.get(jobID.valkeyKey(for: self)).map { ByteBuffer($0) }
    }

    func delete(jobIDs: [JobID]) async throws {
        _ = try await self.valkeyClient.del(keys: jobIDs.map { $0.valkeyKey(for: self) })
    }

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
        public func next() async throws -> Element? {
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
                keys: [self.configuration.pendingQueueKey, self.configuration.cancelledQueueKey],
                args: [jobID.description, "\(Date.now.timeIntervalSince1970)"]
            )
        } else {
            _ = try await self.valkeyClient.execute(
                ZREM(self.configuration.pendingQueueKey, members: [jobID]),
                DEL(keys: [jobID.valkeyKey(for: self)])
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
            keys: [self.configuration.pendingQueueKey, self.configuration.pausedQueueKey],
            args: [jobID.description]
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
            keys: [self.configuration.pausedQueueKey, self.configuration.pendingQueueKey],
            args: [jobID.description]
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
