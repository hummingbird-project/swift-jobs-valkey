// swift-tools-version:6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "swift-jobs-valkey",
    platforms: [.macOS(.v15), .iOS(.v18), .tvOS(.v18)],
    products: [
        .library(name: "JobsValkey", targets: ["JobsValkey"])
    ],
    dependencies: [
        .package(url: "https://github.com/hummingbird-project/swift-jobs.git", branch: "main"),
        .package(url: "https://github.com/valkey-io/valkey-swift", from: "0.5.0"),
    ],
    targets: [
        .target(
            name: "JobsValkey",
            dependencies: [
                .product(name: "Jobs", package: "swift-jobs"),
                .product(name: "Valkey", package: "valkey-swift"),
            ]
        ),
        .testTarget(
            name: "JobsValkeyTests",
            dependencies: [
                .byName(name: "JobsValkey"),
                .product(name: "Jobs", package: "swift-jobs"),
            ]
        ),
    ]
)
