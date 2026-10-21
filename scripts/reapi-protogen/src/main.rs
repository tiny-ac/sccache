// Copyright 2026 Mozilla Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Generates the Rust code for the Remote Execution API from its protobuf files.
//!
//! Usage: `reapi-protogen <proto root> <output dir>`. See `generate.sh`.

fn main() {
    let mut args = std::env::args().skip(1);
    let (Some(proto_root), Some(out_dir)) = (args.next(), args.next()) else {
        eprintln!("usage: reapi-protogen <proto root> <output dir>");
        std::process::exit(2);
    };
    let protos = [
        "build/bazel/remote/execution/v2/remote_execution.proto",
        "google/bytestream/bytestream.proto",
        "google/rpc/error_details.proto",
    ];
    // protox is pure Rust, so the output doesn't depend on an installed protoc.
    let descriptors = protox::compile(protos, [&proto_root]).expect("protobuf compilation failed");
    tonic_prost_build::configure()
        .build_client(true)
        // For fake REAPI servers in tests.
        .build_server(true)
        .server_mod_attribute(".", "#[cfg(test)]")
        .build_transport(false)
        .out_dir(&out_dir)
        .compile_fds(descriptors)
        .expect("code generation failed");
}
