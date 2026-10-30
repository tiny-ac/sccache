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

use super::auth_token::AuthToken;
use super::proto::build::bazel::remote::execution::v2 as remexec;
use crate::errors::*;
use prost::Message;
use remexec::capabilities_client::CapabilitiesClient;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue, BinaryMetadataValue};
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub(super) const RPC_TIMEOUT: Duration = Duration::from_secs(30);
/// The largest response to a call that answers with an `ActionResult`. Servers inline stdout
/// and stderr up to their own message limit, which is often above tonic's 4 MiB default.
pub(super) const MAX_ACTION_RESULT_RESPONSE_BYTES: usize = 32 << 20;
/// Tries after the first for a call that a busy or restarting server refused.
pub(super) const RETRIES: u32 = 2;
/// The wait before the first retry, doubled before the next. Short in tests.
const RETRY_BACKOFF: Duration = if cfg!(test) {
    Duration::from_millis(1)
} else {
    Duration::from_millis(200)
};
/// A call that took longer to fail isn't tried again: a busy server answers at once, while a
/// server that can't be reached takes the connect timeout to fail each time. Short in tests.
const RETRIED_FAILURE_MAX: Duration = if cfg!(test) {
    Duration::from_millis(200)
} else {
    Duration::from_secs(1)
};
// TCP keepalive, since grpc-go and grpc-java servers close connections that send
// HTTP/2 pings more often than every five minutes.
const TCP_KEEPALIVE_IDLE: Duration = Duration::from_secs(30);
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const TCP_KEEPALIVE_RETRIES: u32 = 3;

const REQUIRED_API_MAJOR_VERSION: i32 = 2;
/// v2.1 added `Command.output_paths`, the only output field sccache's commands set.
const MIN_API_MINOR_VERSION: i32 = 1;
/// Used when the server doesn't advertise a batch limit.
const DEFAULT_MAX_BATCH_BYTES: usize = 2 * 1024 * 1024;
/// The `RequestMetadata` mnemonic of a compile's action, as rules_rust names it.
pub(super) const RUSTC_MNEMONIC: &str = "Rustc";
/// The mnemonic of the action that runs `rustc -vV` on a worker.
pub(super) const RUSTC_VERSION_MNEMONIC: &str = "RustcVersion";
/// Carries a [`remexec::RequestMetadata`], which a server logs to tell its clients, their runs
/// and their actions apart.
pub(super) const REQUEST_METADATA_HEADER: &str =
    "build.bazel.remote.execution.v2.requestmetadata-bin";

#[derive(Clone, Default)]
pub(crate) struct ConnectOptions {
    /// `grpc://host:port`, or `grpcs://host:port` for TLS verified against the system's
    /// root certificates, or against `tls_ca_certificates` when set.
    pub(crate) endpoint: String,
    pub(crate) instance_name: String,
    /// Sent as a bearer token with every request.
    pub(crate) auth_token: Option<Arc<AuthToken>>,
    /// Sent with every request.
    pub(crate) headers: BTreeMap<String, String>,
    /// Sent in every request's `RequestMetadata`, so that the server can tell this sccache
    /// server's calls from those of earlier runs and other clients.
    pub(crate) tool_invocation_id: String,
    /// PEM certificates that TLS trusts instead of the system's root certificates.
    pub(crate) tls_ca_certificates: Option<Certificate>,
    /// The PEM certificate and key TLS sends to a server that asks for a client certificate.
    pub(crate) tls_client_identity: Option<Identity>,
}

pub(crate) type ReapiChannel = InterceptedService<Channel, HeadersInterceptor>;

/// Adds the auth token and `headers` to every request, and `request_metadata` to a request
/// that has no `RequestMetadata` of its own.
#[derive(Clone)]
pub(crate) struct HeadersInterceptor {
    auth_token: Option<Arc<AuthToken>>,
    headers: Arc<[(AsciiMetadataKey, AsciiMetadataValue)]>,
    request_metadata: BinaryMetadataValue,
}

impl Interceptor for HeadersInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, tonic::Status> {
        if let Some(auth_token) = &self.auth_token {
            request
                .metadata_mut()
                .insert("authorization", auth_token.header());
        }
        for (name, value) in self.headers.iter() {
            request.metadata_mut().insert(name.clone(), value.clone());
        }
        if !request.metadata().contains_key(REQUEST_METADATA_HEADER) {
            request
                .metadata_mut()
                .insert_bin(REQUEST_METADATA_HEADER, self.request_metadata.clone());
        }
        Ok(request)
    }
}

/// Runs `call`, and again up to [`RETRIES`] times while it fails within
/// [`RETRIED_FAILURE_MAX`] with UNAVAILABLE or RESOURCE_EXHAUSTED, which a busy or
/// restarting server answers.
pub(super) async fn with_retries<T, F, U>(mut call: F) -> Result<T>
where
    F: FnMut() -> U,
    U: Future<Output = Result<T>>,
{
    let mut backoff = RETRY_BACKOFF;
    for _ in 0..RETRIES {
        let started = tokio::time::Instant::now();
        match call().await {
            Err(e)
                if started.elapsed() <= RETRIED_FAILURE_MAX
                    && matches!(
                        rpc_code(&e),
                        Some(tonic::Code::Unavailable | tonic::Code::ResourceExhausted)
                    ) =>
            {
                debug!("REAPI: {e:#}; trying again in {backoff:?}");
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
            result => return result,
        }
    }
    call().await
}

/// Adds `description` to the error if `future` fails or takes longer than `timeout`.
pub(crate) async fn with_timeout<T>(
    description: &str,
    timeout: Duration,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    match tokio::time::timeout(timeout, future).await {
        Ok(result) => result.with_context(|| description.to_owned()),
        Err(_) => bail!("{description}: timed out after {timeout:?}"),
    }
}

/// Error for a REAPI server that lacks REAPI v2.1, SHA-256 digests or caching.
#[derive(Debug)]
pub(crate) struct IncompatibleServer(String);

impl std::fmt::Display for IncompatibleServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for IncompatibleServer {}

/// Error for a REAPI server that refuses the auth token, or a request without one.
#[derive(Debug)]
pub(crate) struct CredentialsRefused;

impl std::fmt::Display for CredentialsRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the REAPI server refused the credentials")
    }
}

/// A connection to a REAPI server.
pub(crate) struct Client {
    instance_name: String,
    channel: ReapiChannel,
    request_metadata: remexec::RequestMetadata,
    max_batch_bytes: usize,
    supports_zstd_bytestream: bool,
    executes_actions: bool,
}

impl Client {
    /// Fails with [`IncompatibleServer`] if the server lacks REAPI v2.1, SHA-256 digests or
    /// caching.
    pub(crate) async fn connect(options: &ConnectOptions) -> Result<Self> {
        let (channel_builder, tls_domain) = channel_endpoint(options, true)?;
        let channel_builder = channel_builder
            .connect_timeout(CONNECT_TIMEOUT)
            .tcp_keepalive(Some(TCP_KEEPALIVE_IDLE))
            .tcp_keepalive_interval(Some(TCP_KEEPALIVE_INTERVAL))
            .tcp_keepalive_retries(Some(TCP_KEEPALIVE_RETRIES));
        if tls_domain.is_none() && (options.auth_token.is_some() || !options.headers.is_empty()) {
            warn!("REAPI: sending the auth token or headers without TLS");
        }
        let headers = request_headers(options)?.into();
        let channel = with_timeout(
            "failed to connect to the REAPI server",
            CONNECT_TIMEOUT,
            async {
                channel_builder
                    .connect()
                    .await
                    .map_err(|e| without_host(e.into(), tls_domain.as_deref()))
            },
        )
        .await?;
        let request_metadata = remexec::RequestMetadata {
            tool_details: Some(remexec::ToolDetails {
                tool_name: "sccache".to_owned(),
                tool_version: env!("CARGO_PKG_VERSION").to_owned(),
            }),
            tool_invocation_id: options.tool_invocation_id.clone(),
            ..Default::default()
        };
        let channel = InterceptedService::new(
            channel,
            HeadersInterceptor {
                auth_token: options.auth_token.clone(),
                headers,
                request_metadata: BinaryMetadataValue::from_bytes(
                    &request_metadata.encode_to_vec(),
                ),
            },
        );

        let capabilities = with_timeout(
            "failed to get the REAPI server's capabilities",
            RPC_TIMEOUT,
            async {
                let request = remexec::GetCapabilitiesRequest {
                    instance_name: options.instance_name.clone(),
                };
                Ok(CapabilitiesClient::new(channel.clone())
                    .get_capabilities(request)
                    .await
                    .map_err(|status| {
                        let refused = matches!(
                            status.code(),
                            tonic::Code::Unauthenticated | tonic::Code::PermissionDenied
                        );
                        let e = status_error(status);
                        if refused {
                            e.context(CredentialsRefused)
                        } else {
                            e
                        }
                    })?
                    .into_inner())
            },
        )
        .await?;
        check_api_version(&capabilities)?;
        let Some(cache_capabilities) = capabilities.cache_capabilities else {
            return Err(Error::new(IncompatibleServer(
                "the REAPI server doesn't offer caching".to_owned(),
            )));
        };
        // Servers from before `digest_functions` existed leave it empty and use SHA-256.
        let sha256 = remexec::digest_function::Value::Sha256 as i32;
        if !cache_capabilities.digest_functions.is_empty()
            && !cache_capabilities.digest_functions.contains(&sha256)
        {
            return Err(Error::new(IncompatibleServer(
                "the REAPI server doesn't support SHA-256 digests".to_owned(),
            )));
        }
        let supports_zstd_bytestream = cache_capabilities
            .supported_compressors
            .contains(&(remexec::compressor::Value::Zstd as i32));
        let max_batch_bytes = match usize::try_from(cache_capabilities.max_batch_total_size_bytes) {
            Ok(max_batch_bytes) if max_batch_bytes > 0 => max_batch_bytes,
            _ => DEFAULT_MAX_BATCH_BYTES,
        };
        // Servers from before `digest_functions` existed set `digest_function` only.
        let executes_actions = capabilities
            .execution_capabilities
            .is_some_and(|execution| {
                execution.exec_enabled
                    && (execution.digest_function == sha256
                        || execution.digest_functions.contains(&sha256))
            });
        info!(
            "REAPI: connected (executes actions: {executes_actions}, \
             zstd ByteStream: {supports_zstd_bytestream}, max batch: {max_batch_bytes} bytes)"
        );
        Ok(Client {
            instance_name: options.instance_name.clone(),
            channel,
            request_metadata,
            max_batch_bytes,
            supports_zstd_bytestream,
            executes_actions,
        })
    }

    pub(crate) fn instance_name(&self) -> &str {
        &self.instance_name
    }

    /// A clone of the channel; every clone shares one connection.
    pub(crate) fn channel(&self) -> ReapiChannel {
        self.channel.clone()
    }

    /// The `RequestMetadata` of every call that isn't for one action.
    pub(crate) fn request_metadata(&self) -> &remexec::RequestMetadata {
        &self.request_metadata
    }

    pub(crate) fn max_batch_bytes(&self) -> usize {
        self.max_batch_bytes
    }

    pub(crate) fn supports_zstd_bytestream(&self) -> bool {
        self.supports_zstd_bytestream
    }

    pub(crate) fn executes_actions(&self) -> bool {
        self.executes_actions
    }
}

/// `message` as a request whose `RequestMetadata` is `request_metadata` with the ID and
/// mnemonic of the action the call is for.
pub(super) fn action_request<T>(
    message: T,
    request_metadata: &remexec::RequestMetadata,
    action_digest: &remexec::Digest,
    action_mnemonic: &str,
) -> tonic::Request<T> {
    let request_metadata = remexec::RequestMetadata {
        action_id: action_digest.hash.clone(),
        action_mnemonic: action_mnemonic.to_owned(),
        ..request_metadata.clone()
    };
    let mut request = tonic::Request::new(message);
    request.metadata_mut().insert_bin(
        REQUEST_METADATA_HEADER,
        BinaryMetadataValue::from_bytes(&request_metadata.encode_to_vec()),
    );
    request
}

/// Fails if `options` has an invalid endpoint, header or TLS file, without connecting.
/// The system's root certificates aren't loaded: a store that is missing or slow to read is a
/// reason to retry the connection, not an invalid config.
pub(super) fn check_connect_options(options: &ConnectOptions) -> Result<()> {
    channel_endpoint(options, false)?;
    request_headers(options)?;
    Ok(())
}

/// `options.headers`, for every request. A header's value is left out of errors, since it may
/// be a credential.
fn request_headers(
    options: &ConnectOptions,
) -> Result<Vec<(AsciiMetadataKey, AsciiMetadataValue)>> {
    let mut headers = Vec::new();
    for (name, value) in &options.headers {
        let key = AsciiMetadataKey::from_bytes(name.as_bytes())
            .with_context(|| format!("the REAPI header name {name:?} is not valid"))?;
        // gRPC and HTTP/2 own these: tonic drops or replaces some, and HTTP/2 refuses
        // requests that have the others.
        if key.as_str().starts_with("grpc-")
            || [
                "te",
                "content-type",
                "user-agent",
                "connection",
                "keep-alive",
                "proxy-connection",
                "transfer-encoding",
                "upgrade",
            ]
            .contains(&key.as_str())
        {
            bail!("the REAPI header {name:?} is reserved for gRPC or HTTP/2");
        }
        if headers.iter().any(|(existing, _)| *existing == key)
            || (key == "authorization" && options.auth_token.is_some())
        {
            bail!("the REAPI header {name:?} is set twice, or with auth_token_file");
        }
        let value = sensitive_ascii_value(value).with_context(|| {
            format!("the REAPI header {name:?} has a non-ASCII or control character")
        })?;
        headers.push((key, value));
    }
    Ok(headers)
}

/// The tonic endpoint for `options`, with TLS set up for `grpcs://`, and the host name TLS
/// verifies. Without `system_roots`, TLS trusts only `tls_ca_certificates`.
fn channel_endpoint(
    options: &ConnectOptions,
    system_roots: bool,
) -> Result<(Endpoint, Option<String>)> {
    let parsed = parse_endpoint(&options.endpoint)?;
    let mut channel_builder =
        Endpoint::from_shared(parsed.http_url).context("the REAPI endpoint is not valid")?;
    match &parsed.tls_domain {
        Some(tls_domain) => {
            let tls_config = ClientTlsConfig::new().domain_name(tls_domain);
            let tls_config = match &options.tls_ca_certificates {
                Some(certificates) => tls_config.ca_certificate(certificates.clone()),
                None if system_roots => tls_config.with_native_roots(),
                None => tls_config,
            };
            let tls_config = match &options.tls_client_identity {
                Some(identity) => tls_config.identity(identity.clone()),
                None => tls_config,
            };
            channel_builder = channel_builder
                .tls_config(tls_config)
                .context("failed to configure TLS for the REAPI server")?;
        }
        None if options.tls_ca_certificates.is_some() || options.tls_client_identity.is_some() => {
            bail!("the REAPI endpoint must start with grpcs:// to use a TLS file")
        }
        None => {}
    }
    Ok((channel_builder, parsed.tls_domain))
}

/// `value` as a header value that tonic leaves out of logs, or `None` if it has a non-ASCII or
/// control character. tonic takes bytes above 0x7f, which servers may not.
pub(super) fn sensitive_ascii_value(value: &str) -> Option<AsciiMetadataValue> {
    let mut value = AsciiMetadataValue::try_from(value)
        .ok()
        .filter(|_| value.is_ascii())?;
    value.set_sensitive(true);
    Some(value)
}

/// Error for a failed REAPI call. It keeps the status's code and message and drops the
/// source, which can name the REAPI server's host.
#[derive(Debug)]
pub(crate) struct RpcError {
    pub(crate) code: tonic::Code,
    pub(crate) message: String,
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, loggable(self.message.as_bytes()))
    }
}

/// Text from the server for an error or a log line: escaped, so that it can't fake log lines
/// or send terminal controls, and cut to 200 chars.
pub(super) fn loggable(text: &[u8]) -> String {
    let text: String = String::from_utf8_lossy(text).chars().take(200).collect();
    format!("{text:?}")
}

impl std::error::Error for RpcError {}

pub(super) fn status_error(status: tonic::Status) -> Error {
    Error::new(RpcError {
        code: status.code(),
        message: status.message().to_owned(),
    })
}

/// The code of the REAPI call that failed with `e`, if `e` is one.
pub(super) fn rpc_code(e: &Error) -> Option<tonic::Code> {
    e.downcast_ref::<RpcError>().map(|e| e.code)
}

/// `error` with each mention of `host` replaced, since rustls names the expected host when the
/// server's certificate doesn't match it.
fn without_host(error: Error, host: Option<&str>) -> Error {
    match host {
        Some(host) => anyhow!("{}", format!("{error:#}").replace(host, "<REAPI server>")),
        None => error,
    }
}

/// A REAPI endpoint as tonic connects to it.
#[derive(Debug, PartialEq)]
struct ParsedEndpoint {
    http_url: String,
    /// The host name TLS verifies, for `grpcs://` endpoints.
    tls_domain: Option<String>,
}

/// Maps `grpc://` to `http://` and `grpcs://` to `https://`. Refuses endpoints without a port,
/// and endpoints with credentials, a path, a query or a fragment, which could carry secrets
/// into logs.
fn parse_endpoint(endpoint: &str) -> Result<ParsedEndpoint> {
    let (http_url, use_tls) = if let Some(rest) = endpoint.strip_prefix("grpc://") {
        (format!("http://{rest}"), false)
    } else if let Some(rest) = endpoint.strip_prefix("grpcs://") {
        (format!("https://{rest}"), true)
    } else {
        bail!("the REAPI endpoint must start with grpc:// or grpcs://")
    };
    let uri: http::Uri = http_url
        .parse()
        .context("the REAPI endpoint is not a valid URL")?;
    let authority = match uri.authority() {
        None => bail!("the REAPI endpoint has no host"),
        Some(authority) if authority.as_str().contains('@') => {
            bail!("the REAPI endpoint must not contain credentials")
        }
        Some(authority) => authority,
    };
    // Without a valid port, the connection would go to port 80 or 443.
    if authority.port_u16().is_none() {
        bail!("the REAPI endpoint must have a port, as in grpcs://host:port");
    }
    // `http::Uri` drops fragments, so the raw endpoint is checked for '#'.
    if !matches!(uri.path(), "" | "/") || uri.query().is_some() || endpoint.contains('#') {
        bail!("the REAPI endpoint must not have a path, query or fragment");
    }
    // TLS verifies an IPv6 address without its brackets.
    let tls_domain = use_tls.then(|| {
        authority
            .host()
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_owned()
    });
    Ok(ParsedEndpoint {
        http_url,
        tls_domain,
    })
}

/// Fails with [`IncompatibleServer`] if the server doesn't support REAPI v2.1. A server
/// that reports no versions passes.
fn check_api_version(capabilities: &remexec::ServerCapabilities) -> Result<()> {
    let lowest_major = capabilities
        .deprecated_api_version
        .as_ref()
        .or(capabilities.low_api_version.as_ref())
        .map(|version| version.major);
    let highest_major = capabilities
        .high_api_version
        .as_ref()
        .map(|version| version.major);
    if lowest_major.is_some_and(|lowest_major| lowest_major > REQUIRED_API_MAJOR_VERSION)
        || highest_major.is_some_and(|highest_major| highest_major < REQUIRED_API_MAJOR_VERSION)
    {
        let format_major =
            |major: Option<i32>| major.map_or("?".to_owned(), |major| format!("v{major}"));
        return Err(Error::new(IncompatibleServer(format!(
            "the REAPI server supports API {} to {}; sccache needs v{REQUIRED_API_MAJOR_VERSION}",
            format_major(lowest_major),
            format_major(highest_major),
        ))));
    }
    if let Some(highest) = &capabilities.high_api_version
        && highest.major == REQUIRED_API_MAJOR_VERSION
        && highest.minor < MIN_API_MINOR_VERSION
    {
        return Err(Error::new(IncompatibleServer(format!(
            "the REAPI server supports API up to v{}.{}; sccache needs \
             v{REQUIRED_API_MAJOR_VERSION}.{MIN_API_MINOR_VERSION}",
            highest.major, highest.minor
        ))));
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::reapi::auth_token::test::{read_again_now, token_file};
    use crate::reapi::proto::build::bazel::semver::SemVer;
    use remexec::capabilities_server::{Capabilities, CapabilitiesServer};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};
    use test_case::test_case;
    use tokio::net::TcpListener;
    use tonic::transport::ServerTlsConfig;
    use tonic::transport::server::TcpIncoming;

    /// Answers GetCapabilities with `capabilities` and records each request's instance name,
    /// `authorization` header and metadata.
    struct FakeCapabilities {
        capabilities: remexec::ServerCapabilities,
        instance_names: Arc<Mutex<Vec<String>>>,
        authorization_headers: Arc<Mutex<Vec<Option<String>>>>,
        metadata: Arc<Mutex<Vec<tonic::metadata::MetadataMap>>>,
    }

    impl Default for FakeCapabilities {
        fn default() -> Self {
            FakeCapabilities {
                capabilities: remexec::ServerCapabilities {
                    cache_capabilities: Some(Default::default()),
                    ..Default::default()
                },
                instance_names: Default::default(),
                authorization_headers: Default::default(),
                metadata: Default::default(),
            }
        }
    }

    #[tonic::async_trait]
    impl Capabilities for FakeCapabilities {
        async fn get_capabilities(
            &self,
            request: tonic::Request<remexec::GetCapabilitiesRequest>,
        ) -> std::result::Result<tonic::Response<remexec::ServerCapabilities>, tonic::Status>
        {
            let authorization_header = request
                .metadata()
                .get("authorization")
                .map(|header| header.to_str().unwrap().to_owned());
            self.authorization_headers
                .lock()
                .unwrap()
                .push(authorization_header);
            self.metadata
                .lock()
                .unwrap()
                .push(request.metadata().clone());
            self.instance_names
                .lock()
                .unwrap()
                .push(request.into_inner().instance_name);
            Ok(tonic::Response::new(self.capabilities.clone()))
        }
    }

    /// Serves `fake` on a local port and returns its `grpc://` endpoint.
    async fn serve_fake(fake: FakeCapabilities) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .serve_with_incoming(CapabilitiesServer::new(fake), TcpIncoming::from(listener)),
        );
        format!("grpc://{addr}")
    }

    /// `tests/reapi-tls`, whose files `generate.sh` there writes.
    fn tls_dir() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/reapi-tls")
    }

    fn tls_file(name: &str) -> Vec<u8> {
        std::fs::read(tls_dir().join(name)).unwrap()
    }

    /// Runs `f` with the system's root certificates replaced by the certificates in `file`.
    fn with_system_roots<T>(file: &std::path::Path, f: impl FnOnce() -> T) -> T {
        // rustls-native-certs reads this file instead of the system's store.
        temp_env::with_var("SSL_CERT_FILE", Some(file), f)
    }

    /// Serves `fake` over TLS, with a certificate for 127.0.0.1 that the test CA signed, and
    /// returns its `grpcs://` endpoint. With `client_certificate_required`, the server only
    /// accepts clients that send a certificate the test CA signed.
    async fn serve_fake_over_tls(
        fake: FakeCapabilities,
        client_certificate_required: bool,
    ) -> String {
        // tonic's server uses rustls' process-wide provider, which rustls can't pick itself
        // when both ring and aws-lc-rs are built.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let tls_config = ServerTlsConfig::new().identity(Identity::from_pem(
            tls_file("server.pem"),
            tls_file("server.key"),
        ));
        let tls_config = if client_certificate_required {
            tls_config.client_ca_root(Certificate::from_pem(tls_file("ca.pem")))
        } else {
            tls_config
        };
        tokio::spawn(
            tonic::transport::Server::builder()
                .tls_config(tls_config)
                .unwrap()
                .serve_with_incoming(CapabilitiesServer::new(fake), TcpIncoming::from(listener)),
        );
        format!("grpcs://{addr}")
    }

    fn auth_token(token: &str) -> Arc<AuthToken> {
        Arc::new(AuthToken::fixed(token).unwrap())
    }

    fn connect_options(endpoint: &str) -> ConnectOptions {
        ConnectOptions {
            endpoint: endpoint.to_owned(),
            ..Default::default()
        }
    }

    fn api_version(major: i32, minor: i32) -> Option<SemVer> {
        Some(SemVer {
            major,
            minor,
            ..Default::default()
        })
    }

    #[test]
    fn test_a_status_message_is_escaped() {
        let e = status_error(tonic::Status::internal("ok\nINFO forged\x1b[2J"));
        assert_eq!(r#"Internal: "ok\nINFO forged\u{1b}[2J""#, e.to_string());
    }

    #[tokio::test]
    async fn test_connect_sends_instance_name_and_reads_capabilities() {
        let fake = FakeCapabilities {
            capabilities: remexec::ServerCapabilities {
                cache_capabilities: Some(remexec::CacheCapabilities {
                    max_batch_total_size_bytes: 4 * 1024 * 1024,
                    supported_compressors: vec![remexec::compressor::Value::Zstd as i32],
                    ..Default::default()
                }),
                execution_capabilities: Some(remexec::ExecutionCapabilities {
                    exec_enabled: true,
                    digest_functions: vec![remexec::digest_function::Value::Sha256 as i32],
                    ..Default::default()
                }),
                low_api_version: api_version(2, 0),
                high_api_version: api_version(2, 3),
                ..Default::default()
            },
            ..Default::default()
        };
        let instance_names = fake.instance_names.clone();
        let endpoint = serve_fake(fake).await;
        let client = Client::connect(&ConnectOptions {
            instance_name: "main".to_owned(),
            ..connect_options(&endpoint)
        })
        .await
        .unwrap();
        assert_eq!(vec!["main".to_owned()], *instance_names.lock().unwrap());
        assert_eq!(4 * 1024 * 1024, client.max_batch_bytes());
        assert!(client.supports_zstd_bytestream());
        assert!(client.executes_actions());
    }

    #[tokio::test]
    async fn test_connect_defaults_what_the_server_doesnt_advertise() {
        let endpoint = serve_fake(FakeCapabilities::default()).await;
        let client = Client::connect(&connect_options(&endpoint)).await.unwrap();
        assert_eq!(DEFAULT_MAX_BATCH_BYTES, client.max_batch_bytes());
        assert!(!client.supports_zstd_bytestream());
        assert!(!client.executes_actions());
    }

    const SHA256: i32 = remexec::digest_function::Value::Sha256 as i32;
    const MD5: i32 = remexec::digest_function::Value::Md5 as i32;

    #[test_case(true, SHA256, vec![], true ; "sha256 in the old field")]
    #[test_case(false, SHA256, vec![SHA256], false ; "execution disabled")]
    #[test_case(true, MD5, vec![MD5], false ; "without sha256")]
    #[tokio::test]
    async fn test_executes_actions_needs_execution_enabled_with_sha256(
        exec_enabled: bool,
        digest_function: i32,
        digest_functions: Vec<i32>,
        executes_actions: bool,
    ) {
        let endpoint = serve_fake(FakeCapabilities {
            capabilities: remexec::ServerCapabilities {
                cache_capabilities: Some(Default::default()),
                execution_capabilities: Some(remexec::ExecutionCapabilities {
                    exec_enabled,
                    digest_function,
                    digest_functions,
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        })
        .await;
        let client = Client::connect(&connect_options(&endpoint)).await.unwrap();
        assert_eq!(executes_actions, client.executes_actions());
    }

    #[tokio::test]
    async fn test_auth_token_is_sent_as_a_bearer_token() {
        let fake = FakeCapabilities::default();
        let authorization_headers = fake.authorization_headers.clone();
        let endpoint = serve_fake(fake).await;
        Client::connect(&ConnectOptions {
            auth_token: Some(auth_token("secret\n")),
            ..connect_options(&endpoint)
        })
        .await
        .unwrap();
        Client::connect(&connect_options(&endpoint)).await.unwrap();
        assert_eq!(
            vec![Some("Bearer secret".to_owned()), None],
            *authorization_headers.lock().unwrap()
        );
    }

    #[tokio::test]
    async fn test_grpcs_does_not_fall_back_to_plain_text() {
        let fake = FakeCapabilities::default();
        let authorization_headers = fake.authorization_headers.clone();
        let endpoint = serve_fake(fake).await.replace("grpc://", "grpcs://");
        let e = Client::connect(&ConnectOptions {
            auth_token: Some(auth_token("secret")),
            ..connect_options(&endpoint)
        })
        .await;
        assert!(e.is_err());
        // The auth token never reached the plain-text server.
        assert!(authorization_headers.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_grpcs_trusts_the_configured_ca() {
        let fake = FakeCapabilities::default();
        let authorization_headers = fake.authorization_headers.clone();
        let endpoint = serve_fake_over_tls(fake, false).await;
        Client::connect(&ConnectOptions {
            auth_token: Some(auth_token("secret")),
            tls_ca_certificates: Some(Certificate::from_pem(tls_file("ca.pem"))),
            ..connect_options(&endpoint)
        })
        .await
        .unwrap();
        assert_eq!(
            vec![Some("Bearer secret".to_owned())],
            *authorization_headers.lock().unwrap()
        );
    }

    #[test_case("ca.pem", None, true ; "system roots that trust the server")]
    #[test_case("other-ca.pem", None, false ; "system roots that dont")]
    #[test_case("ca.pem", Some("other-ca.pem"), false ; "a ca file replaces the system roots")]
    #[test_case("other-ca.pem", Some("ca.pem"), true ; "a ca file that trusts the server")]
    #[serial_test::serial(reqwest_tls_env)]
    fn test_grpcs_trusts_the_ca_file_or_else_the_system_roots(
        system_roots: &str,
        tls_ca_file: Option<&str>,
        connects: bool,
    ) {
        with_system_roots(&tls_dir().join(system_roots), || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let endpoint = serve_fake_over_tls(FakeCapabilities::default(), false).await;
                let connected = Client::connect(&ConnectOptions {
                    tls_ca_certificates: tls_ca_file
                        .map(|file| Certificate::from_pem(tls_file(file))),
                    ..connect_options(&endpoint)
                })
                .await
                .is_ok();
                assert_eq!(connects, connected);
            });
        });
    }

    #[tokio::test]
    async fn test_a_certificate_for_another_host_is_refused_without_naming_the_host() {
        // The server's certificate is only for 127.0.0.1.
        let endpoint = serve_fake_over_tls(FakeCapabilities::default(), false)
            .await
            .replace("127.0.0.1", "localhost");
        let e = Client::connect(&ConnectOptions {
            tls_ca_certificates: Some(Certificate::from_pem(tls_file("ca.pem"))),
            ..connect_options(&endpoint)
        })
        .await
        .err()
        .unwrap();
        let e = format!("{e:#}");
        assert!(
            e.contains("<REAPI server>") && !e.contains("localhost"),
            "{e}"
        );
    }

    #[test]
    #[serial_test::serial(reqwest_tls_env)]
    fn test_options_are_checked_without_the_system_roots() {
        with_system_roots(&tls_dir().join("missing.pem"), || {
            check_connect_options(&connect_options("grpcs://farm:443")).unwrap();
        });
    }

    fn client_identity() -> Identity {
        Identity::from_pem(tls_file("client.pem"), tls_file("client.key"))
    }

    #[test_case(true ; "with a client certificate")]
    #[test_case(false ; "without a client certificate")]
    #[tokio::test]
    async fn test_a_server_that_requires_a_client_certificate_gets_it(
        sends_client_certificate: bool,
    ) {
        let endpoint = serve_fake_over_tls(FakeCapabilities::default(), true).await;
        let connected = Client::connect(&ConnectOptions {
            tls_ca_certificates: Some(Certificate::from_pem(tls_file("ca.pem"))),
            tls_client_identity: sends_client_certificate.then(client_identity),
            ..connect_options(&endpoint)
        })
        .await
        .is_ok();
        assert_eq!(sends_client_certificate, connected);
    }

    #[test]
    fn test_a_client_key_that_doesnt_match_the_certificate_is_refused_without_connecting() {
        let e = check_connect_options(&ConnectOptions {
            tls_client_identity: Some(Identity::from_pem(
                tls_file("client.pem"),
                tls_file("server.key"),
            )),
            ..connect_options("grpcs://farm:443")
        })
        .unwrap_err();
        // rustls' InconsistentKeys error.
        assert!(
            format!("{e:#}").contains("keys may not be consistent"),
            "{e:#}"
        );
    }

    #[test_case(Some(Certificate::from_pem(tls_file("ca.pem"))), None ; "ca")]
    #[test_case(None, Some(client_identity()) ; "client certificate")]
    fn test_tls_files_need_grpcs(
        tls_ca_certificates: Option<Certificate>,
        tls_client_identity: Option<Identity>,
    ) {
        let e = check_connect_options(&ConnectOptions {
            tls_ca_certificates,
            tls_client_identity,
            ..connect_options("grpc://farm:8980")
        })
        .unwrap_err();
        assert!(format!("{e}").contains("grpcs://"), "{e}");
    }

    #[tokio::test]
    async fn test_a_changed_token_is_sent_from_then_on() {
        let dir = tempfile::tempdir().unwrap();
        let (auth_token, write_token) = token_file(dir.path(), Duration::ZERO);
        let fake = FakeCapabilities::default();
        let authorization_headers = fake.authorization_headers.clone();
        let endpoint = serve_fake(fake).await;
        let client = Client::connect(&ConnectOptions {
            auth_token: Some(auth_token.clone()),
            ..connect_options(&endpoint)
        })
        .await
        .unwrap();
        write_token("second");
        read_again_now(&auth_token).await;
        CapabilitiesClient::new(client.channel())
            .get_capabilities(remexec::GetCapabilitiesRequest::default())
            .await
            .unwrap();
        assert_eq!(
            vec![
                Some("Bearer first".to_owned()),
                Some("Bearer second".to_owned())
            ],
            *authorization_headers.lock().unwrap()
        );
    }

    #[tokio::test]
    async fn test_configured_headers_are_sent_with_the_auth_token() {
        let fake = FakeCapabilities::default();
        let metadata = fake.metadata.clone();
        let endpoint = serve_fake(fake).await;
        Client::connect(&ConnectOptions {
            auth_token: Some(auth_token("secret")),
            headers: [("X-Api-Key".to_owned(), "key".to_owned())].into(),
            ..connect_options(&endpoint)
        })
        .await
        .unwrap();
        let metadata = &metadata.lock().unwrap()[0];
        assert_eq!("key", metadata.get("x-api-key").unwrap());
        assert_eq!("Bearer secret", metadata.get("authorization").unwrap());
    }

    #[test_case("x api key", "hidden", "not valid" ; "invalid name")]
    #[test_case("x-api-key-bin", "hidden", "not valid" ; "binary name")]
    #[test_case("x-api-key", "sec\nret", "control character" ; "control character in the value")]
    #[test_case("x-api-key", "hédden", "non-ASCII" ; "non-ascii value")]
    #[test_case("grpc-timeout", "1S", "reserved" ; "grpc name")]
    #[test_case("Content-Type", "text/plain", "reserved" ; "content type")]
    #[test_case("Connection", "close", "reserved" ; "http/2 connection header")]
    #[test_case("Authorization", "Bearer other", "set twice" ; "authorization with a token")]
    fn test_a_bad_header_is_refused_without_echoing_its_value(
        name: &str,
        value: &str,
        error: &str,
    ) {
        let e = check_connect_options(&ConnectOptions {
            auth_token: Some(auth_token("secret")),
            headers: [(name.to_owned(), value.to_owned())].into(),
            ..connect_options("grpc://farm:8980")
        })
        .unwrap_err();
        let e = format!("{e:#}");
        assert!(
            e.contains(error) && !e.contains(value) && !e.contains("secret"),
            "{e}"
        );
    }

    #[test_case(3, "v3 to v3" ; "newer only")]
    #[test_case(1, "v1 to v1" ; "older only")]
    #[tokio::test]
    async fn test_server_without_api_v2_is_refused(major: i32, expected_range: &str) {
        let endpoint = serve_fake(FakeCapabilities {
            capabilities: remexec::ServerCapabilities {
                cache_capabilities: Some(Default::default()),
                low_api_version: api_version(major, 0),
                high_api_version: api_version(major, 0),
                ..Default::default()
            },
            ..Default::default()
        })
        .await;
        let e = Client::connect(&connect_options(&endpoint))
            .await
            .err()
            .unwrap();
        assert!(e.to_string().contains(expected_range), "{e}");
        assert!(e.is::<IncompatibleServer>(), "{e}");
    }

    /// A server whose newest API version is v2.`highest_minor`.
    async fn serve_v2(highest_minor: i32) -> String {
        serve_fake(FakeCapabilities {
            capabilities: remexec::ServerCapabilities {
                cache_capabilities: Some(Default::default()),
                low_api_version: api_version(2, 0),
                high_api_version: api_version(2, highest_minor),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
    }

    #[tokio::test]
    async fn test_server_with_api_v2_0_only_is_refused() {
        let endpoint = serve_v2(0).await;
        let e = Client::connect(&connect_options(&endpoint))
            .await
            .err()
            .unwrap();
        assert!(
            e.to_string().contains("up to v2.0; sccache needs v2.1"),
            "{e}"
        );
        assert!(e.is::<IncompatibleServer>(), "{e}");
    }

    #[tokio::test]
    async fn test_server_with_api_v2_1_is_accepted() {
        let endpoint = serve_v2(1).await;
        Client::connect(&connect_options(&endpoint)).await.unwrap();
    }

    #[tokio::test]
    async fn test_server_without_sha256_is_refused() {
        let endpoint = serve_fake(FakeCapabilities {
            capabilities: remexec::ServerCapabilities {
                cache_capabilities: Some(remexec::CacheCapabilities {
                    digest_functions: vec![remexec::digest_function::Value::Blake3 as i32],
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        })
        .await;
        let e = Client::connect(&connect_options(&endpoint))
            .await
            .err()
            .unwrap();
        assert!(e.to_string().contains("SHA-256"), "{e}");
        assert!(e.is::<IncompatibleServer>(), "{e}");
    }

    #[tokio::test]
    async fn test_server_with_sha256_among_others_is_accepted() {
        let endpoint = serve_fake(FakeCapabilities {
            capabilities: remexec::ServerCapabilities {
                cache_capabilities: Some(remexec::CacheCapabilities {
                    digest_functions: vec![
                        remexec::digest_function::Value::Blake3 as i32,
                        remexec::digest_function::Value::Sha256 as i32,
                    ],
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        })
        .await;
        Client::connect(&connect_options(&endpoint)).await.unwrap();
    }

    #[tokio::test]
    async fn test_server_without_caching_is_refused() {
        let endpoint = serve_fake(FakeCapabilities {
            capabilities: remexec::ServerCapabilities::default(),
            ..Default::default()
        })
        .await;
        let e = Client::connect(&connect_options(&endpoint))
            .await
            .err()
            .unwrap();
        assert!(e.to_string().contains("caching"), "{e}");
        assert!(e.is::<IncompatibleServer>(), "{e}");
    }

    #[tokio::test]
    async fn test_server_with_api_v2_deprecated_is_accepted() {
        let endpoint = serve_fake(FakeCapabilities {
            capabilities: remexec::ServerCapabilities {
                cache_capabilities: Some(Default::default()),
                deprecated_api_version: api_version(2, 0),
                low_api_version: api_version(3, 0),
                high_api_version: api_version(3, 0),
                ..Default::default()
            },
            ..Default::default()
        })
        .await;
        Client::connect(&connect_options(&endpoint)).await.unwrap();
    }

    #[test]
    fn test_status_error_keeps_the_code_and_leaves_out_the_source() {
        let mut status = tonic::Status::unavailable("transport error");
        status.set_source(std::sync::Arc::new(std::io::Error::other(
            "certificate not valid for name reapi.example.com",
        )));
        assert!(status.to_string().contains("reapi.example.com"));
        let e = status_error(status);
        assert_eq!(r#"Unavailable: "transport error""#, e.to_string());
        assert_eq!(Some(tonic::Code::Unavailable), rpc_code(&e));
    }

    #[test_case(tonic::Code::Unavailable, RETRIES + 1 ; "unavailable")]
    #[test_case(tonic::Code::ResourceExhausted, RETRIES + 1 ; "resource exhausted")]
    #[test_case(tonic::Code::Internal, 1 ; "internal")]
    #[test_case(tonic::Code::NotFound, 1 ; "not found")]
    #[test_case(tonic::Code::Unauthenticated, 1 ; "unauthenticated")]
    #[test_case(tonic::Code::PermissionDenied, 1 ; "permission denied")]
    #[tokio::test]
    async fn test_only_unavailable_and_resource_exhausted_are_tried_again(
        code: tonic::Code,
        tries: u32,
    ) {
        let calls = &AtomicU32::new(0);
        let e = with_retries(|| async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(status_error(tonic::Status::new(code, "no")))
        })
        .await
        .unwrap_err();
        assert_eq!(Some(code), rpc_code(&e));
        assert_eq!(tries, calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_a_slow_failure_is_not_tried_again() {
        let calls = &AtomicU32::new(0);
        with_retries(|| async move {
            calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(RETRIED_FAILURE_MAX + Duration::from_millis(50)).await;
            Err::<(), _>(status_error(tonic::Status::unavailable("unreachable")))
        })
        .await
        .unwrap_err();
        assert_eq!(1, calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_a_call_the_server_answers_on_a_retry_succeeds() {
        let calls = &AtomicU32::new(0);
        let answer = with_retries(|| async move {
            if calls.fetch_add(1, Ordering::SeqCst) < RETRIES {
                return Err(status_error(tonic::Status::unavailable("busy")));
            }
            Ok("answer")
        })
        .await
        .unwrap();
        assert_eq!("answer", answer);
    }

    #[test]
    fn test_without_host_replaces_the_host() {
        let error =
            anyhow!("certificate not valid for name reapi.example.com").context("transport error");
        assert_eq!(
            "transport error: certificate not valid for name <REAPI server>",
            without_host(error, Some("reapi.example.com")).to_string()
        );
    }

    #[tokio::test]
    async fn test_with_timeout_adds_the_description_to_errors() {
        let e = with_timeout("asking", RPC_TIMEOUT, async {
            Err::<(), _>(anyhow!("refused"))
        })
        .await
        .unwrap_err();
        assert_eq!("asking: refused", format!("{e:#}"));
    }

    #[tokio::test]
    async fn test_with_timeout_fails_after_the_timeout() {
        let timeout = Duration::from_millis(1);
        let e = with_timeout("asking", timeout, std::future::pending::<Result<()>>())
            .await
            .unwrap_err();
        assert_eq!("asking: timed out after 1ms", e.to_string());
    }

    #[test]
    fn test_header_values_are_left_out_of_logs() {
        let headers = request_headers(&ConnectOptions {
            headers: [("x-api-key".to_owned(), "key".to_owned())].into(),
            ..Default::default()
        })
        .unwrap();
        assert!(headers[0].1.is_sensitive());
    }

    #[test_case("grpc://farm:8980", "http://farm:8980", None ; "plain text")]
    #[test_case("grpcs://farm:443", "https://farm:443", Some("farm") ; "tls")]
    #[test_case("grpcs://[::1]:443", "https://[::1]:443", Some("::1") ; "tls to an ipv6 address")]
    fn test_grpc_endpoint_is_mapped_to_http(
        endpoint: &str,
        http_url: &str,
        tls_domain: Option<&str>,
    ) {
        assert_eq!(
            ParsedEndpoint {
                http_url: http_url.to_owned(),
                tls_domain: tls_domain.map(str::to_owned),
            },
            parse_endpoint(endpoint).unwrap()
        );
    }

    #[test_case("grpc://farm" ; "missing")]
    #[test_case("grpc://farm:" ; "empty")]
    #[test_case("grpc://farm:abc" ; "not a number")]
    #[test_case("grpc://farm:99999" ; "out of range")]
    fn test_endpoint_without_a_valid_port_is_refused(endpoint: &str) {
        let e = parse_endpoint(endpoint).unwrap_err();
        assert!(format!("{e:#}").contains("must have a port"), "{e:#}");
    }

    #[test_case("https://user:secret@farm:443" ; "https scheme with credentials")]
    #[test_case("farm:8980" ; "no scheme")]
    #[test_case("grpcs://user:secret@farm:443" ; "credentials")]
    #[test_case("grpc:///user:secret@farm" ; "credentials without a host")]
    #[test_case("grpcs://farm/secret" ; "path")]
    #[test_case("grpc://farm:8980?token=secret" ; "query")]
    #[test_case("grpc://farm:8980#secret" ; "fragment")]
    fn test_endpoint_is_refused_without_echoing_it(endpoint: &str) {
        let e = parse_endpoint(endpoint).unwrap_err();
        assert!(!format!("{e:#}").contains("secret"), "{e:#}");
    }

    #[test_case("grpcs://farm:443", true ; "valid")]
    #[test_case("farm:443", false ; "bad endpoint")]
    fn test_connect_options_are_checked_without_connecting(endpoint: &str, valid: bool) {
        let options = ConnectOptions {
            endpoint: endpoint.to_owned(),
            auth_token: Some(auth_token("token")),
            ..Default::default()
        };
        assert_eq!(valid, check_connect_options(&options).is_ok());
    }
}
