// swift-tools-version:6.1
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

var defaultSwiftSettings: [SwiftSetting] = [
    // https://github.com/apple/swift-evolution/blob/main/proposals/0335-existential-any.md
    .enableUpcomingFeature("ExistentialAny"),

    // https://github.com/swiftlang/swift-evolution/blob/main/proposals/0444-member-import-visibility.md
    .enableUpcomingFeature("MemberImportVisibility"),

    // https://github.com/swiftlang/swift-evolution/blob/main/proposals/0409-access-level-on-imports.md
    .enableUpcomingFeature("InternalImportsByDefault"),
]

#if compiler(>=6.2)
defaultSwiftSettings.append(contentsOf: [
    // https://github.com/swiftlang/swift-evolution/blob/main/proposals/0461-async-function-isolation.md
    .enableUpcomingFeature("NonisolatedNonsendingByDefault")
])
#endif

let package = Package(
    name: "swift-jobs-valkey",
    platforms: [.macOS(.v15), .iOS(.v18), .tvOS(.v18)],
    products: [
        .library(name: "JobsValkey", targets: ["JobsValkey"])
    ],
    dependencies: [
        .package(url: "https://github.com/hummingbird-project/swift-jobs.git", from: "1.1.0"),
        .package(url: "https://github.com/valkey-io/valkey-swift", from: "1.0.0"),
    ],
    targets: [
        .target(
            name: "JobsValkey",
            dependencies: [
                .product(name: "Jobs", package: "swift-jobs"),
                .product(name: "Valkey", package: "valkey-swift"),
            ],
            swiftSettings: defaultSwiftSettings
        ),
        .testTarget(
            name: "JobsValkeyTests",
            dependencies: [
                .byName(name: "JobsValkey"),
                .product(name: "Jobs", package: "swift-jobs"),
            ],
            swiftSettings: defaultSwiftSettings
        ),
    ]
)
