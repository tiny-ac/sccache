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

use super::proto::build::bazel::remote::execution::v2 as remexec;
use crate::errors::*;
use remexec::capabilities_client::CapabilitiesClient;
use std::future::Future;
use std::time::Duration;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub(super) const RPC_TIMEOUT: Duration = Duration::from_secs(30);
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

#[derive(Clone, Default)]
pub(crate) struct ConnectOptions {
    /// `grpc://host:port`, or `grpcs://host:port` for TLS verified against the system's
    /// root certificates.
    pub(crate) endpoint: String,
    pub(crate) instance_name: String,
    /// Sent as a bearer token with every request. Surrounding whitespace is ignored.
    pub(crate) auth_token: Option<String>,
}

pub(crate) type AuthChannel = InterceptedService<Channel, AuthInterceptor>;

/// Adds `auth_header`, when set, to every request.
#[derive(Clone)]
pub(crate) struct AuthInterceptor {
    auth_header: Option<AsciiMetadataValue>,
}

impl Interceptor for AuthInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, tonic::Status> {
        if let Some(auth_header) = &self.auth_header {
            request
                .metadata_mut()
                .insert("authorization", auth_header.clone());
        }
        Ok(request)
    }
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
    channel: AuthChannel,
    max_batch_bytes: usize,
    supports_zstd_bytestream: bool,
}

impl Client {
    /// Fails with [`IncompatibleServer`] if the server lacks REAPI v2.1, SHA-256 digests or
    /// caching.
    pub(crate) async fn connect(options: &ConnectOptions) -> Result<Self> {
        let endpoint = parse_endpoint(&options.endpoint)?;
        let mut channel_builder = Endpoint::from_shared(endpoint.http_url)
            .context("the REAPI endpoint is not valid")?
            .connect_timeout(CONNECT_TIMEOUT)
            .tcp_keepalive(Some(TCP_KEEPALIVE_IDLE))
            .tcp_keepalive_interval(Some(TCP_KEEPALIVE_INTERVAL))
            .tcp_keepalive_retries(Some(TCP_KEEPALIVE_RETRIES));
        if let Some(tls_domain) = &endpoint.tls_domain {
            channel_builder = channel_builder
                .tls_config(
                    ClientTlsConfig::new()
                        .with_native_roots()
                        .domain_name(tls_domain),
                )
                .context("failed to configure TLS for the REAPI server")?;
        } else if options.auth_token.is_some() {
            warn!("REAPI: sending the auth token without TLS");
        }
        let auth_header = auth_header(options.auth_token.as_deref())?;
        let channel = with_timeout(
            "failed to connect to the REAPI server",
            CONNECT_TIMEOUT,
            async {
                channel_builder
                    .connect()
                    .await
                    .map_err(|e| without_host(e.into(), endpoint.tls_domain.as_deref()))
            },
        )
        .await?;
        let channel = InterceptedService::new(channel, AuthInterceptor { auth_header });

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
        info!(
            "REAPI: connected (zstd ByteStream: {supports_zstd_bytestream}, \
             max batch: {max_batch_bytes} bytes)"
        );
        Ok(Client {
            instance_name: options.instance_name.clone(),
            channel,
            max_batch_bytes,
            supports_zstd_bytestream,
        })
    }

    pub(crate) fn instance_name(&self) -> &str {
        &self.instance_name
    }

    /// A clone of the channel; every clone shares one connection.
    pub(crate) fn channel(&self) -> AuthChannel {
        self.channel.clone()
    }

    pub(crate) fn max_batch_bytes(&self) -> usize {
        self.max_batch_bytes
    }

    pub(crate) fn supports_zstd_bytestream(&self) -> bool {
        self.supports_zstd_bytestream
    }
}

/// Fails if `options` has an invalid endpoint or auth token, without connecting.
pub(super) fn check_connect_options(options: &ConnectOptions) -> Result<()> {
    let endpoint = parse_endpoint(&options.endpoint)?;
    Endpoint::from_shared(endpoint.http_url).context("the REAPI endpoint is not valid")?;
    auth_header(options.auth_token.as_deref())?;
    Ok(())
}

/// The bearer `authorization` header for `auth_token`, if any.
fn auth_header(auth_token: Option<&str>) -> Result<Option<AsciiMetadataValue>> {
    match auth_token.map(str::trim) {
        Some("") => bail!("the REAPI auth token is empty"),
        Some(token) => {
            let mut header = AsciiMetadataValue::try_from(format!("Bearer {token}"))
                .context("the REAPI auth token contains a non-ASCII or control character")?;
            header.set_sensitive(true);
            Ok(Some(header))
        }
        None => Ok(None),
    }
}

/// `status`'s code and message, without its source: a transport error there can name the
/// REAPI server's host.
pub(super) fn status_error(status: tonic::Status) -> Error {
    anyhow!("{:?}: {}", status.code(), status.message())
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
    use crate::reapi::proto::build::bazel::semver::SemVer;
    use remexec::capabilities_server::{Capabilities, CapabilitiesServer};
    use std::sync::{Arc, Mutex};
    use test_case::test_case;
    use tokio::net::TcpListener;
    use tonic::transport::server::TcpIncoming;

    /// Answers GetCapabilities with `capabilities` and records each request's instance name and
    /// `authorization` header.
    struct FakeCapabilities {
        capabilities: remexec::ServerCapabilities,
        instance_names: Arc<Mutex<Vec<String>>>,
        authorization_headers: Arc<Mutex<Vec<Option<String>>>>,
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

    #[tokio::test]
    async fn test_connect_sends_instance_name_and_reads_capabilities() {
        let fake = FakeCapabilities {
            capabilities: remexec::ServerCapabilities {
                cache_capabilities: Some(remexec::CacheCapabilities {
                    max_batch_total_size_bytes: 4 * 1024 * 1024,
                    supported_compressors: vec![remexec::compressor::Value::Zstd as i32],
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
    }

    #[tokio::test]
    async fn test_connect_defaults_what_the_server_doesnt_advertise() {
        let endpoint = serve_fake(FakeCapabilities::default()).await;
        let client = Client::connect(&connect_options(&endpoint)).await.unwrap();
        assert_eq!(DEFAULT_MAX_BATCH_BYTES, client.max_batch_bytes());
        assert!(!client.supports_zstd_bytestream());
    }

    #[tokio::test]
    async fn test_auth_token_is_sent_as_a_bearer_token() {
        let fake = FakeCapabilities::default();
        let authorization_headers = fake.authorization_headers.clone();
        let endpoint = serve_fake(fake).await;
        Client::connect(&ConnectOptions {
            auth_token: Some("secret\n".to_owned()),
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
            auth_token: Some("secret".to_owned()),
            ..connect_options(&endpoint)
        })
        .await;
        assert!(e.is_err());
        // The auth token never reached the plain-text server.
        assert!(authorization_headers.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_empty_auth_token_is_refused() {
        let e = Client::connect(&ConnectOptions {
            auth_token: Some(" \n".to_owned()),
            ..connect_options("grpc://127.0.0.1:1")
        })
        .await
        .err()
        .unwrap();
        assert!(e.to_string().contains("auth token is empty"), "{e}");
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
    fn test_status_error_leaves_out_the_source() {
        let mut status = tonic::Status::unavailable("transport error");
        status.set_source(std::sync::Arc::new(std::io::Error::other(
            "certificate not valid for name reapi.example.com",
        )));
        assert!(status.to_string().contains("reapi.example.com"));
        assert_eq!(
            "Unavailable: transport error",
            status_error(status).to_string()
        );
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

    #[tokio::test]
    async fn test_auth_token_with_a_control_character_is_refused_without_echoing_it() {
        let e = Client::connect(&ConnectOptions {
            auth_token: Some("sec\nret".to_owned()),
            ..connect_options("grpc://127.0.0.1:1")
        })
        .await
        .err()
        .unwrap();
        assert!(!format!("{e:#}").contains("sec"), "{e:#}");
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

    #[test_case("grpcs://farm:443", Some("token"), true ; "valid")]
    #[test_case("farm:443", None, false ; "bad endpoint")]
    #[test_case("grpcs://farm:443", Some(" "), false ; "empty token")]
    fn test_connect_options_are_checked_without_connecting(
        endpoint: &str,
        auth_token: Option<&str>,
        valid: bool,
    ) {
        let options = ConnectOptions {
            endpoint: endpoint.to_owned(),
            auth_token: auth_token.map(str::to_owned),
            ..Default::default()
        };
        assert_eq!(valid, check_connect_options(&options).is_ok());
    }
}
