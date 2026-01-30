//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Logging
import NIOCore
import Valkey

extension ValkeyJobQueue {
    static let FunctionVersion = 1
    /// Upload Valkey JobQueue lua functions to server.
    ///
    /// This includes functions
    /// - swiftjobs_pop: Get latest job
    /// - swiftjobs_cancel: Called when cancelling a job
    /// - swiftjobs_cancelAndRetain: Called when cancelling a job and cancelled jobs are retained
    /// - swiftjobs_pauseResume: Called when moving between paused and non-paused state
    /// - swiftjobs_rerunQueue: Called during job cleanup and moving the contents of processing set
    ///        back onto the pending queue.
    ///
    /// If running the job queue handler this is done automatically. If you are not
    /// running the job queue handler you should call this to ensure the job queue
    /// functions are available.
    public func loadFunctions() async throws {
        // Only load function if they don't exist or the version number is different
        do {
            let version = try await self.valkeyClient.fcall(function: "swiftjobs_version").decode(as: Int.self)
            if version == Self.FunctionVersion {
                return
            }
        } catch let error as ValkeyClientError where error.errorCode == .commandError {
        }
        try await self.loadFunctions.acquire {
            _ = try await self.valkeyClient.functionLoad(
                replace: true,
                functionCode: """
                    #!lua name=_swift_jobs_valkey

                    local function version()
                        return 24
                    end

                    local function pop(keys, args)
                        local values = redis.call("ZPOPMIN", keys[1], args[1])
                        if #(values) == 0 then 
                            return nil
                        end
                        local readyJobs = {}
                        local delayedJobs = {}
                        for i = 1,#(values),2 do
                            if tonumber(values[2]) > tonumber(args[2]) then
                                table.insert(delayedJobs, values[i+1])
                                table.insert(delayedJobs, values[i])
                            else
                                table.insert(readyJobs, values[i])
                            end
                        end
                        if #(delayedJobs) > 0 then
                            redis.call("ZADD", keys[1], unpack(delayedJobs))
                        end
                        if #(readyJobs) > 0 then
                            redis.call("LPUSH", keys[2], unpack(readyJobs))
                            return readyJobs
                        else
                            return nil
                        end
                    end

                    local function cancel(keys, args)
                        if redis.call("ZREM", keys[1], args[1]) > 0 then
                            redis.call("DEL", keys[2])
                        end
                        return redis.status_reply('OK')
                    end

                    local function cancelAndRetain(keys, args)
                        if redis.call("ZREM", keys[1], args[1]) > 0 then
                            redis.call("ZADD", keys[2], args[2], args[1])
                        end
                        return redis.status_reply('OK')
                    end

                    local function pauseResume(keys, args)
                        local score = redis.call("ZSCORE", keys[1], args[1])
                        if score == nil then
                            return nil
                        end
                        redis.call("ZREM", keys[1], args[1])
                        redis.call("ZADD", keys[2], score, args[1])
                        return redis.status_reply('OK')
                    end

                    local function rerunQueue(keys, args)
                        while true do
                            local value = redis.call("RPOP", keys[1])
                            if value == false then
                                break
                            end
                            redis.call("ZADD", keys[2], 0, value)
                        end
                        return redis.status_reply('OK')
                    end

                    server.register_function('swiftjobs_version', version)
                    server.register_function('swiftjobs_pop', pop)
                    server.register_function('swiftjobs_cancel', cancel)
                    server.register_function('swiftjobs_cancelAndRetain', cancelAndRetain)
                    server.register_function('swiftjobs_pauseResume', pauseResume)
                    server.register_function('swiftjobs_rerunQueue', rerunQueue)
                    """
            )
        }
    }
}
