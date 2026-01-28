//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Synchronization
import Testing

@testable import JobsValkey

struct AsyncInitializedGlobalTests {
    final class Box<Value: ~Copyable & Sendable>: Sendable {
        let value: Value

        init(_ value: consuming Value) {
            self.value = value
        }
    }
    @Test
    func testAcquire() async throws {
        let counter = Atomic(0)
        let global = AsyncInitializedGlobal<Int>()
        let value = try await global.acquire {
            counter.add(1, ordering: .relaxed).newValue
        }
        #expect(value == 1)
        #expect(counter.load(ordering: .relaxed) == 1)
    }

    @Test
    func testAcquireAfterAcquired() async throws {
        let counter = Atomic(0)
        let global = AsyncInitializedGlobal<Int>()
        let value = try await global.acquire {
            counter.add(1, ordering: .relaxed).newValue
        }
        #expect(value == 1)
        let value2 = try await global.acquire {
            counter.add(1, ordering: .relaxed).newValue
        }
        #expect(value2 == 1)
    }

    @Test
    func testConcurrentAcquires() async throws {
        let counter = Box(Atomic(0))
        let global = Box(AsyncInitializedGlobal<Int>())
        await withThrowingTaskGroup(of: Void.self) { group in
            for _ in 0..<8 {
                group.addTask {
                    let value = try await global.value.acquire {
                        try await Task.sleep(for: .milliseconds(1))
                        return counter.value.add(1, ordering: .relaxed).newValue
                    }
                    #expect(value == 1)
                }
            }
        }
    }

    @Test
    func testConcurrentErroringAcquires() async throws {
        struct TestError: Error {}
        let global = Box(AsyncInitializedGlobal<Int>())
        try await withThrowingTaskGroup(of: Int.self) { group in
            for _ in 0..<8 {
                group.addTask {
                    try await global.value.acquire {
                        try await Task.sleep(for: .milliseconds(1))
                        throw TestError()
                    }
                }
            }
            while true {
                do {
                    if try await group.next() == nil { return }
                    Issue.record("Global should not return a value")
                } catch is TestError {
                }
            }
        }
    }

    @Test
    func testCancellingAcquire() async throws {
        let counter = Box(Atomic(0))
        let global = Box(AsyncInitializedGlobal<Int>())
        let task1 = Task {
            try await global.value.acquire {
                try await Task.sleep(for: .milliseconds(2))
                return counter.value.add(1, ordering: .relaxed).newValue
            }
        }
        let task2 = Task {
            try await global.value.acquire {
                try await Task.sleep(for: .milliseconds(2))
                return counter.value.add(1, ordering: .relaxed).newValue
            }
        }
        try await Task.sleep(for: .milliseconds(1))
        task1.cancel()
        let value = try await task2.value
        #expect(value == 1)
    }

    @Test
    func testCancellingAll() async throws {
        let counter = Box(Atomic(0))
        let global = Box(AsyncInitializedGlobal<Int>())
        let task1 = Task {
            try await global.value.acquire {
                try await Task.sleep(for: .milliseconds(2))
                return counter.value.add(1, ordering: .relaxed).newValue
            }
        }
        let task2 = Task {
            try await global.value.acquire {
                try await Task.sleep(for: .milliseconds(2))
                return counter.value.add(1, ordering: .relaxed).newValue
            }
        }
        try await Task.sleep(for: .milliseconds(1))
        task1.cancel()
        task2.cancel()
        let value = try await global.value.acquire {
            try await Task.sleep(for: .milliseconds(2))
            return counter.value.add(1, ordering: .relaxed).newValue
        }
        #expect(value == 1)
    }
}
