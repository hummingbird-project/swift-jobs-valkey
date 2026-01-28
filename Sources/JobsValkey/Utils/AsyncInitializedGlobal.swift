//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Synchronization

struct AsyncInitializedGlobal<Value: Sendable>: ~Copyable, Sendable {
    enum Action {
        case use(Value)
        case acquire
    }

    enum State {
        case uninitialized
        case acquiring([CheckedContinuation<Action, any Error>])
        case acquired(Value)
    }
    let state: Mutex<State>

    init() {
        self.state = .init(.uninitialized)
    }

    @usableFromInline
    func acquire(operation: () async throws -> Value) async throws -> Value {
        let action: Action = try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Action, any Error>) in
            self.state.withLock { state in
                switch state {
                case .uninitialized:
                    state = .acquiring([])
                    cont.resume(returning: .acquire)
                case .acquiring(var continuations):
                    continuations.append(cont)
                    state = .acquiring(continuations)
                case .acquired(let value):
                    cont.resume(returning: .use(value))
                }
            }
        }
        switch action {
        case .acquire:
            do {
                let value = try await operation()
                return self.state.withLock { state in
                    guard case .acquiring(let continuations) = state else {
                        preconditionFailure("State should still be acquiring")
                    }
                    for cont in continuations {
                        cont.resume(returning: .use(value))
                    }
                    state = .acquired(value)
                    return value
                }
            } catch is CancellationError {
                self.state.withLock { state in
                    guard case .acquiring(var continuations) = state else {
                        preconditionFailure("State should still be acquiring")
                    }
                    if let lastContinuation = continuations.popLast() {
                        state = .acquiring(continuations)
                        lastContinuation.resume(returning: .acquire)
                    } else {
                        state = .uninitialized
                    }
                }
                throw CancellationError()
            } catch {
                return try self.state.withLock { state in
                    guard case .acquiring(let continuations) = state else {
                        preconditionFailure("State should still be acquiring")
                    }
                    for cont in continuations {
                        cont.resume(throwing: error)
                    }
                    state = .uninitialized
                    throw error
                }
            }
        case .use(let value):
            return value
        }
    }
}
