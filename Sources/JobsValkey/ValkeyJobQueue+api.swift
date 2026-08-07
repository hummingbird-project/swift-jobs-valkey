//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

#if compiler(>=6.2.3)

@_spi(JobsAPI) public import Jobs
import NIOCore
import NIOFoundationEssentialsCompat
import Valkey

#if canImport(FoundationEssentials)
public import FoundationEssentials
#else
public import Foundation
#endif

/// AnyJob details used by JobsAPI
private struct JobsAPIAnyJob: Decodable {
    /// Data used to define a single instance of a job
    struct InstanceData: Decodable, Sendable {
        /// Time job was queued
        let queuedAt: Date
        /// Current attempt
        @usableFromInline
        let attempt: Int

        // keep JSON strings small to improve decode speed
        private enum CodingKeys: String, CodingKey {
            case queuedAt = "q"
            case attempt = "a"
        }
    }
    let name: String
    let data: InstanceData

    public init(from decoder: any Decoder) throws {
        // Job JSON is structured as follows
        //  {
        //      "JobName": { job data... }
        //  }
        let container = try decoder.container(keyedBy: _JobCodingKey.self)
        guard let key = container.allKeys.first else {
            throw DecodingError.dataCorrupted(.init(codingPath: decoder.codingPath, debugDescription: "No keys found."))
        }
        self.name = key.stringValue
        self.data = try container.decode(InstanceData.self, forKey: key)
    }
}

@_spi(JobsAPI) extension ValkeyJobQueue: JobsAPI {
    public func getJobs(maxNumber: Int, paginationToken: String?) async throws -> GetJobsResponse {
        let cursor = paginationToken.flatMap { Int($0) } ?? 0
        let scanResponse = try await self.valkeyClient.scan(
            cursor: cursor,
            pattern: "\(self.configuration.queueName)/*",
            count: maxNumber,
            type: "string"
        )
        let jobIDStrings = try scanResponse.keys.map { try String($0) }
        let commands: [any ValkeyCommand] = jobIDStrings.flatMap { (id) -> [any ValkeyCommand] in
            [
                GET(.init(id)),
                HGET(.init("\(id).metadata"), field: Self.statusKey),
            ]
        }
        let responses = await self.valkeyClient.execute(commands)
        var jobMetadata: [JobAPIMetadata] = []
        for index in 0..<jobIDStrings.count {
            if let jobID = UUID(uuidString: jobIDStrings[index]) {
                let job = try responses[index * 2].get().decode(as: ByteBuffer.self)
                guard let status = try JobAPIMetadata.Status(rawValue: String(responses[index * 2 + 1].get())) else { continue }
                // has to decode job to get name, should we add a name column to table
                let anyJob = try JSONDecoder().decode(JobsAPIAnyJob.self, from: job)

                jobMetadata.append(
                    .init(id: jobID, name: anyJob.name, createdAt: anyJob.data.queuedAt, completedAt: nil, status: status)
                )
            }
        }
        return .init(
            paginationToken: "\(scanResponse.cursor)",
            jobs: jobMetadata
        )
    }

    public func getJob(id: UUID) async throws -> GetJobResponse? {
        let (jobRequest, metadataRequest) = await self.valkeyClient.execute(
            GET(self.valkeyKey(forJobID: .init(uuid: id))),
            HGET(self.valkeyMetadataKey(forJobID: .init(uuid: id)), field: Self.statusKey)
        )
        guard let job = try jobRequest.get().map({ ByteBuffer($0) }) else { return nil }
        guard let status = try metadataRequest.get().flatMap({ JobAPIMetadata.Status(rawValue: String($0)) }) else { return nil }
        // has to decode job to get name, should we add a name column to table
        let anyJob = try JSONDecoder().decode(JobsAPIAnyJob.self, from: job)
        return GetJobResponse(
            jobMetadata: .init(
                id: id,
                name: anyJob.name,
                createdAt: anyJob.data.queuedAt,
                completedAt: nil,
                status: status
            ),
            jobParameters: job
        )
    }
}

internal struct _JobCodingKey: CodingKey {
    var stringValue: String
    var intValue: Int?

    init?(stringValue: String) {
        self.stringValue = stringValue
        self.intValue = nil
    }

    init?(intValue: Int) {
        self.stringValue = "\(intValue)"
        self.intValue = intValue
    }

    init(stringValue: String, intValue: Int?) {
        self.stringValue = stringValue
        self.intValue = intValue
    }

    internal init(index: Int) {
        self.stringValue = "Index \(index)"
        self.intValue = index
    }
}

#endif
