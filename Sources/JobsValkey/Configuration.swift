//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import NIOCore
import Valkey

extension ValkeyJobQueue {
    /// Valkey Job queue configuration
    public struct Configuration: Sendable {
        /// queue name
        public let queueName: String
        /// Pending queue Valkey key
        public let pendingQueueKey: ValkeyKey
        /// Processing queue Valkey key
        public let processingQueueKey: ValkeyKey
        /// Paused queue Valkey key
        public let pausedQueueKey: ValkeyKey
        /// Failed queue Valkey key
        public let failedQueueKey: ValkeyKey
        /// Cancelled queue Valkey key
        public let cancelledQueueKey: ValkeyKey
        /// Completed queue Valkey key
        public let completedQueueKey: ValkeyKey
        /// Prefix for metadata
        public let metadataKeyPrefix: String
        /// Queue poll time to wait if queue empties
        public let pollTime: Duration
        /// Retention policy for jobs
        public let retentionPolicy: RetentionPolicy
        /// Maximum number of jobs to pop off pending queue at one time. This value should not be greater than the number of workers
        public let maxJobsPoppedFromPendingQueue: Int

        public init(
            queueName: String = "_hbJobQueue",
            pollTime: Duration = .milliseconds(100),
            retentionPolicy: RetentionPolicy = .init(),
            maxJobsPoppedFromPendingQueue: Int = 8
        ) {
            self.queueName = queueName
            self.pendingQueueKey = ValkeyKey("\(queueName).pending")
            self.pausedQueueKey = ValkeyKey("\(queueName).paused")
            self.processingQueueKey = ValkeyKey("\(queueName).processing")
            self.failedQueueKey = ValkeyKey("\(queueName).failed")
            self.cancelledQueueKey = ValkeyKey("\(queueName).cancelled")
            self.completedQueueKey = ValkeyKey("\(queueName).completed")
            self.metadataKeyPrefix = "\(queueName).metadata."
            self.pollTime = pollTime
            self.retentionPolicy = retentionPolicy
            self.maxJobsPoppedFromPendingQueue = maxJobsPoppedFromPendingQueue
        }
    }
}
