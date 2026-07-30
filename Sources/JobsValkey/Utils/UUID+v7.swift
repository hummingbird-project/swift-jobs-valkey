//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift.org open source project
//
// Copyright (c) 2022 Apple Inc. and the Swift project authors
// Licensed under Apache License v2.0 with Runtime Library Exception
//
// See https://swift.org/LICENSE.txt for license information
//
//===----------------------------------------------------------------------===//

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

extension UUID {
    /// Creates a new UUID with RFC 9562 version 7 layout using the specified random number generator for the random bits.
    ///
    /// - Parameters:
    ///   - date: The date to encode in the timestamp field.
    ///   - offset: A duration to add to the timestamp before encoding. Defaults to zero.
    /// - Returns: A version 7 UUID.
    internal static func _version7(at date: Date = .now, offset: Duration = .zero) -> UUID {
        var generator = SystemRandomNumberGenerator()
        // The most significant 48 bits contain a millisecond-precision Unix timestamp.
        // The 12 bits following the version field (`rand_a`) encode sub-millisecond timestamp precision per RFC 9562 Section 6.2, Method 3.
        // The remaining 62 bits (`rand_b`, after the variant field) are filled using `generator`.
        // Caller-provided date (plus offset): convert to Duration,
        // no monotonic guard
        let elapsed = Duration.seconds(date.timeIntervalSince1970) + offset
        let (ms, subMS) = elapsed._uuidTimestampComponents

        var first: UInt64 = 0
        // Bits 0–47: millisecond timestamp
        first |= ms << 16
        // Bits 48–51: version 7 (0111)
        first |= 0x7000
        // Bits 52–63: sub-millisecond precision (12 bits)
        first |= UInt64(subMS)

        // Bits 64–127: variant + random
        var second = UInt64.random(in: .min ... .max, using: &generator)
        // Set the variant to '10' in bits 64–65
        second &= 0x3FFF_FFFF_FFFF_FFFF
        second |= 0x8000_0000_0000_0000

        let uuidBytes = (
            UInt8(truncatingIfNeeded: first >> 56),
            UInt8(truncatingIfNeeded: first >> 48),
            UInt8(truncatingIfNeeded: first >> 40),
            UInt8(truncatingIfNeeded: first >> 32),
            UInt8(truncatingIfNeeded: first >> 24),
            UInt8(truncatingIfNeeded: first >> 16),
            UInt8(truncatingIfNeeded: first >> 8),
            UInt8(truncatingIfNeeded: first),
            UInt8(truncatingIfNeeded: second >> 56),
            UInt8(truncatingIfNeeded: second >> 48),
            UInt8(truncatingIfNeeded: second >> 40),
            UInt8(truncatingIfNeeded: second >> 32),
            UInt8(truncatingIfNeeded: second >> 24),
            UInt8(truncatingIfNeeded: second >> 16),
            UInt8(truncatingIfNeeded: second >> 8),
            UInt8(truncatingIfNeeded: second)
        )
        return UUID(uuid: uuidBytes)
    }
}

extension Duration {
    /// Attoseconds per millisecond (10^15).
    private static let _attosPerMS: Int64 = 1_000_000_000_000_000

    fileprivate var _uuidTimestampComponents: (ms: UInt64, subMS: UInt16) {
        let (secs, attos) = self.components

        // Total milliseconds = seconds * 1000 + attoseconds / attosPerMS
        let totalMS = Int64(secs) * 1000 + attos / Self._attosPerMS

        // Clamp to the 48-bit unsigned range (0 ... 0xFFFF_FFFF_FFFF)
        let ms =
            UInt64(clamping: Swift.max(0, totalMS))
            & 0xFFFF_FFFF_FFFF

        // Sub-millisecond fraction: remaining attoseconds after
        // removing whole milliseconds, scaled to 12 bits.
        let remainingAttos = attos % Self._attosPerMS
        let subMS = UInt16((remainingAttos * 4096) / Self._attosPerMS)

        return (ms, subMS)
    }
}
