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
        Err(_) => bail!("{description} timed out after {timeout:?}"),
    }
}

/// Error for a REAPI server that lacks REAPI v2, SHA-256 digests or caching.
#[derive(Debug)]
pub(crate) struct IncompatibleServer(String);

impl std::fmt::Display for IncompatibleServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for IncompatibleServer {}

/// A connection to a REAPI server.
pub(crate) struct Client {
    instance_name: String,
    channel: AuthChannel,
    supports_zstd_bytestream: bool,
}

impl Client {
    /// Fails with [`IncompatibleServer`] if the server lacks REAPI v2, SHA-256 digests or
    /// caching.
    pub(crate) async fn connect(options: &ConnectOptions) -> Result<Self> {
        let (http_url, use_tls) = parse_endpoint(&options.endpoint)?;
        let mut channel_builder = Endpoint::from_shared(http_url)
            .context("the REAPI endpoint is not valid")?
            .connect_timeout(CONNECT_TIMEOUT)
            .tcp_keepalive(Some(TCP_KEEPALIVE_IDLE))
            .tcp_keepalive_interval(Some(TCP_KEEPALIVE_INTERVAL))
            .tcp_keepalive_retries(Some(TCP_KEEPALIVE_RETRIES));
        if use_tls {
            channel_builder = channel_builder
                .tls_config(ClientTlsConfig::new().with_native_roots())
                .context("configuring TLS for the REAPI endpoint")?;
        } else if options.auth_token.is_some() {
            warn!("REAPI: sending the auth token without TLS");
        }
        let auth_header = match options.auth_token.as_deref().map(str::trim) {
            Some("") => bail!("the REAPI auth token is empty"),
            Some(token) => {
                let mut header = AsciiMetadataValue::try_from(format!("Bearer {token}"))
                    .context("the REAPI auth token contains a non-ASCII or control character")?;
                header.set_sensitive(true);
                Some(header)
            }
            None => None,
        };
        let channel = with_timeout("connecting to the REAPI endpoint", CONNECT_TIMEOUT, async {
            Ok(channel_builder.connect().await?)
        })
        .await?;
        let channel = InterceptedService::new(channel, AuthInterceptor { auth_header });

        let capabilities = with_timeout(
            "asking the REAPI server for its capabilities",
            RPC_TIMEOUT,
            async {
                let request = remexec::GetCapabilitiesRequest {
                    instance_name: options.instance_name.clone(),
                };
                Ok(CapabilitiesClient::new(channel.clone())
                    .get_capabilities(request)
                    .await?
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
        // Servers that list no digest functions are assumed to support SHA-256.
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
        info!("REAPI: connected (zstd ByteStream: {supports_zstd_bytestream})");
        Ok(Client {
            instance_name: options.instance_name.clone(),
            channel,
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

    pub(crate) fn supports_zstd_bytestream(&self) -> bool {
        self.supports_zstd_bytestream
    }
}

/// Maps `grpc://` to `http://` and `grpcs://` to `https://`, returning true for TLS.
/// Rejects endpoints with credentials, a path, a query or a fragment.
fn parse_endpoint(endpoint: &str) -> Result<(String, bool)> {
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
    match uri.authority() {
        None => bail!("the REAPI endpoint has no host"),
        Some(authority) if authority.as_str().contains('@') => {
            bail!("the REAPI endpoint must not contain credentials")
        }
        Some(_) => {}
    }
    // `http::Uri` drops fragments.
    if !matches!(uri.path(), "" | "/") || uri.query().is_some() || endpoint.contains('#') {
        bail!("the REAPI endpoint must not have a path, query or fragment");
    }
    Ok((http_url, use_tls))
}

/// Fails with [`IncompatibleServer`] if the server doesn't support REAPI v2. A server
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

    fn api_version(major: i32) -> Option<SemVer> {
        Some(SemVer {
            major,
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn test_connect_sends_instance_name_and_detects_zstd() {
        let fake = FakeCapabilities {
            capabilities: remexec::ServerCapabilities {
                cache_capabilities: Some(remexec::CacheCapabilities {
                    supported_compressors: vec![remexec::compressor::Value::Zstd as i32],
                    ..Default::default()
                }),
                low_api_version: api_version(2),
                high_api_version: api_version(2),
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
        assert!(client.supports_zstd_bytestream());
    }

    #[tokio::test]
    async fn test_server_without_zstd_does_not_support_zstd_bytestream() {
        let endpoint = serve_fake(FakeCapabilities::default()).await;
        let client = Client::connect(&connect_options(&endpoint)).await.unwrap();
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
        .await
        .err();
        assert!(e.is_some());
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
                low_api_version: api_version(major),
                high_api_version: api_version(major),
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
                deprecated_api_version: api_version(2),
                low_api_version: api_version(3),
                high_api_version: api_version(3),
                ..Default::default()
            },
            ..Default::default()
        })
        .await;
        Client::connect(&connect_options(&endpoint)).await.unwrap();
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
        assert_eq!("asking timed out after 1ms", e.to_string());
    }

    #[test_case("grpc://farm:8980", "http://farm:8980", false ; "plain text")]
    #[test_case("grpcs://farm:443", "https://farm:443", true ; "tls")]
    fn test_grpc_endpoint_is_mapped_to_http(endpoint: &str, http_url: &str, use_tls: bool) {
        assert_eq!(
            (http_url.to_owned(), use_tls),
            parse_endpoint(endpoint).unwrap()
        );
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
}
