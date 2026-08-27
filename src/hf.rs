use bytes::Bytes;
use futures::Stream;
use reqwest::Client;

use crate::error::Hfs3Error;
use crate::types::{HfFileEntry, RepoRef, RepoType};

pub const HF_BASE: &str = "https://huggingface.co";

/// Build the HF tree API URL for listing repo files.
fn api_url_with_base(base: &str, repo: &RepoRef) -> String {
    let type_segment = match repo.repo_type {
        RepoType::Model => "models",
        RepoType::Dataset => "datasets",
        RepoType::Space => "spaces",
    };
    format!(
        "{}/api/{}/{}/tree/{}?recursive=true",
        base, type_segment, repo.repo_id, repo.revision
    )
}

/// Build the HF resolve URL for downloading a file.
fn download_url_with_base(base: &str, repo: &RepoRef, file_path: &str) -> String {
    let type_prefix = match repo.repo_type {
        RepoType::Model => String::new(),
        RepoType::Dataset => "datasets/".to_string(),
        RepoType::Space => "spaces/".to_string(),
    };
    format!(
        "{}/{}{}/resolve/{}/{}",
        base, type_prefix, repo.repo_id, repo.revision, file_path
    )
}

/// Map an HTTP status from an HF API or file request to an actionable error.
fn hf_http_error(status: reqwest::StatusCode, url: &str, repo: &RepoRef) -> Hfs3Error {
    match status.as_u16() {
        401 | 403 => Hfs3Error::HfApi(format!(
            "HF access denied ({status}) for '{}': the repo needs a token (gated/private), or it does not exist (unauthenticated HF also returns 401 for missing repos). Set HF_TOKEN to a valid HuggingFace token; if the repo ID was given bare, include the type prefix (e.g. 'datasets/owner/name' or 'spaces/owner/name') so it is looked up in the right namespace.",
            repo.repo_id
        )),
        404 => Hfs3Error::HfApi(format!(
            "HF repo not found: '{}' (GET {} returned 404)",
            repo.repo_id, url
        )),
        _ => Hfs3Error::HfApi(format!("GET {} returned {}", url, status)),
    }
}

/// JSON shape returned by the HF tree API (superset of what we need).
#[derive(serde::Deserialize)]
struct HfTreeEntry {
    #[serde(rename = "type")]
    entry_type: String,
    path: String,
    #[serde(default)]
    size: u64,
    #[serde(default)]
    oid: String,
}

/// Detect the correct repo type by probing the HF API.
///
/// Tries model, space, then dataset in order (model is most common).
/// Returns the first type that returns a success status, or an error
/// if none match.
pub async fn detect_repo_type(
    client: &Client,
    repo_id: &str,
    revision: &str,
    token: Option<&str>,
) -> Result<RepoType, Hfs3Error> {
    detect_repo_type_with_base(client, HF_BASE, repo_id, revision, token).await
}

pub async fn detect_repo_type_with_base(
    client: &Client,
    base_url: &str,
    repo_id: &str,
    revision: &str,
    token: Option<&str>,
) -> Result<RepoType, Hfs3Error> {
    let candidates = [RepoType::Model, RepoType::Space, RepoType::Dataset];

    for repo_type in &candidates {
        let probe = RepoRef {
            repo_id: repo_id.to_string(),
            repo_type: repo_type.clone(),
            revision: revision.to_string(),
        };
        let url = api_url_with_base(base_url, &probe);

        let mut req = client.head(&url);
        if let Some(t) = token {
            req = req.bearer_auth(t);
        }

        match req.send().await {
            Ok(resp) if resp.status().is_success() => {
                tracing::info!(repo_id, %repo_type, "auto-detected repo type");
                return Ok(repo_type.clone());
            }
            _ => continue,
        }
    }

    Err(Hfs3Error::HfApi(format!(
        "could not detect repo type for '{}' — not found as model, space, or dataset",
        repo_id
    )))
}

/// List files in a HuggingFace repo via the tree API.
///
/// Calls `GET /api/{models|datasets|spaces}/{repo_id}/tree/{revision}?recursive=true`,
/// filters to entries with `type == "file"`, and maps to [`HfFileEntry`].
pub async fn list_repo_files(
    client: &Client,
    repo: &RepoRef,
    token: Option<&str>,
) -> Result<Vec<HfFileEntry>, Hfs3Error> {
    list_repo_files_with_base(client, HF_BASE, repo, token).await
}

/// List repo files against a custom base URL (testable).
pub async fn list_repo_files_with_base(
    client: &Client,
    base_url: &str,
    repo: &RepoRef,
    token: Option<&str>,
) -> Result<Vec<HfFileEntry>, Hfs3Error> {
    let url = api_url_with_base(base_url, repo);

    let mut req = client.get(&url);
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }

    let resp = req.send().await?;

    let status = resp.status();
    if !status.is_success() {
        return Err(hf_http_error(status, &url, repo));
    }

    let entries: Vec<HfTreeEntry> = resp.json().await?;

    let files = entries
        .into_iter()
        .filter(|e| e.entry_type == "file")
        .map(|e| HfFileEntry {
            path: e.path,
            size: e.size,
            oid: e.oid,
        })
        .collect();

    Ok(files)
}

/// Download a single file from HuggingFace as a byte stream.
///
/// Returns `(stream, content_length)`. The stream yields `Bytes` chunks
/// suitable for piping into S3 multipart upload.
pub async fn download_file_stream(
    client: &Client,
    repo: &RepoRef,
    file_path: &str,
    token: Option<&str>,
) -> Result<
    (
        impl Stream<Item = Result<Bytes, reqwest::Error>>,
        Option<u64>,
    ),
    Hfs3Error,
> {
    download_file_stream_range(client, repo, file_path, token, 0).await
}

/// Download a file starting at a byte offset (HTTP Range request).
/// Used to resume interrupted uploads without re-downloading completed parts.
pub async fn download_file_stream_range(
    client: &Client,
    repo: &RepoRef,
    file_path: &str,
    token: Option<&str>,
    range_start: u64,
) -> Result<
    (
        impl Stream<Item = Result<Bytes, reqwest::Error>>,
        Option<u64>,
    ),
    Hfs3Error,
> {
    download_file_stream_with_base(client, HF_BASE, repo, file_path, token, range_start).await
}

/// Download a file against a custom base URL (testable).
///
/// `range_start == 0` downloads the whole file; a larger value requests
/// `bytes={range_start}-` and errors if the server does not honor the range.
pub async fn download_file_stream_with_base(
    client: &Client,
    base_url: &str,
    repo: &RepoRef,
    file_path: &str,
    token: Option<&str>,
    range_start: u64,
) -> Result<
    (
        impl Stream<Item = Result<Bytes, reqwest::Error>>,
        Option<u64>,
    ),
    Hfs3Error,
> {
    let url = download_url_with_base(base_url, repo, file_path);

    let mut req = client.get(&url);
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }
    if range_start > 0 {
        req = req.header("Range", format!("bytes={range_start}-"));
    }

    let resp = req.send().await?;

    let status = resp.status();
    if !status.is_success() {
        return Err(hf_http_error(status, &url, repo));
    }
    if range_start > 0 && status.as_u16() != 206 {
        return Err(Hfs3Error::HfApi(format!(
            "server returned {status} instead of 206 for a Range request on {url}; cannot resume safely"
        )));
    }

    let content_length = resp.content_length();
    let stream = resp.bytes_stream();

    Ok((stream, content_length))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model_repo() -> RepoRef {
        RepoRef {
            repo_id: "meta-llama/Llama-2-7b".to_string(),
            repo_type: RepoType::Model,
            revision: "main".to_string(),
        }
    }

    fn dataset_repo() -> RepoRef {
        RepoRef {
            repo_id: "user/my-dataset".to_string(),
            repo_type: RepoType::Dataset,
            revision: "main".to_string(),
        }
    }

    fn space_repo() -> RepoRef {
        RepoRef {
            repo_id: "user/my-space".to_string(),
            repo_type: RepoType::Space,
            revision: "v2".to_string(),
        }
    }

    #[test]
    fn test_api_url_model() {
        let repo = model_repo();
        assert_eq!(
            api_url_with_base(HF_BASE, &repo),
            "https://huggingface.co/api/models/meta-llama/Llama-2-7b/tree/main?recursive=true"
        );
    }

    #[test]
    fn test_api_url_dataset() {
        let repo = dataset_repo();
        assert_eq!(
            api_url_with_base(HF_BASE, &repo),
            "https://huggingface.co/api/datasets/user/my-dataset/tree/main?recursive=true"
        );
    }

    #[test]
    fn test_download_url_model() {
        let repo = model_repo();
        assert_eq!(
            download_url_with_base(HF_BASE, &repo, "config.json"),
            "https://huggingface.co/meta-llama/Llama-2-7b/resolve/main/config.json"
        );
    }

    #[test]
    fn test_download_url_space() {
        let repo = space_repo();
        assert_eq!(
            download_url_with_base(HF_BASE, &repo, "app.py"),
            "https://huggingface.co/spaces/user/my-space/resolve/v2/app.py"
        );
    }

    #[test]
    fn test_api_url_with_base_custom() {
        let repo = model_repo();
        assert_eq!(
            api_url_with_base("http://localhost:1234", &repo),
            "http://localhost:1234/api/models/meta-llama/Llama-2-7b/tree/main?recursive=true"
        );
    }

    mod detect {
        use super::*;
        use wiremock::matchers::{method, path_regex};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        #[tokio::test]
        async fn test_detect_model() {
            let server = MockServer::start().await;

            Mock::given(method("HEAD"))
                .and(path_regex(r"/api/models/.+"))
                .respond_with(ResponseTemplate::new(200))
                .mount(&server)
                .await;

            let client = Client::new();
            let result =
                detect_repo_type_with_base(&client, &server.uri(), "org/some-model", "main", None)
                    .await
                    .unwrap();

            assert_eq!(result, RepoType::Model);
        }

        #[tokio::test]
        async fn test_detect_space_after_model_401() {
            let server = MockServer::start().await;

            // Model probe for a non-model repo → 401 (real HF unauth behavior)
            Mock::given(method("HEAD"))
                .and(path_regex(r"/api/models/.+"))
                .respond_with(ResponseTemplate::new(401))
                .mount(&server)
                .await;

            // Space → 200
            Mock::given(method("HEAD"))
                .and(path_regex(r"/api/spaces/.+"))
                .respond_with(ResponseTemplate::new(200))
                .mount(&server)
                .await;

            let client = Client::new();
            let result =
                detect_repo_type_with_base(&client, &server.uri(), "user/my-app", "main", None)
                    .await
                    .unwrap();

            assert_eq!(result, RepoType::Space);
        }

        #[tokio::test]
        async fn test_detect_dataset_after_model_and_space_401() {
            let server = MockServer::start().await;

            Mock::given(method("HEAD"))
                .and(path_regex(r"/api/models/.+"))
                .respond_with(ResponseTemplate::new(401))
                .mount(&server)
                .await;

            Mock::given(method("HEAD"))
                .and(path_regex(r"/api/spaces/.+"))
                .respond_with(ResponseTemplate::new(401))
                .mount(&server)
                .await;

            Mock::given(method("HEAD"))
                .and(path_regex(r"/api/datasets/.+"))
                .respond_with(ResponseTemplate::new(200))
                .mount(&server)
                .await;

            let client = Client::new();
            let result = detect_repo_type_with_base(
                &client,
                &server.uri(),
                "org/some-dataset",
                "main",
                None,
            )
            .await
            .unwrap();

            assert_eq!(result, RepoType::Dataset);
        }

        #[tokio::test]
        async fn test_detect_all_401_returns_error() {
            let server = MockServer::start().await;

            // Unauthenticated probes for a nonexistent repo all return 401
            Mock::given(method("HEAD"))
                .respond_with(ResponseTemplate::new(401))
                .mount(&server)
                .await;

            let client = Client::new();
            let result = detect_repo_type_with_base(
                &client,
                &server.uri(),
                "ghost/nonexistent",
                "main",
                None,
            )
            .await;

            assert!(result.is_err());
            let msg = result.unwrap_err().to_string();
            assert!(msg.contains("ghost/nonexistent"));
        }

        #[tokio::test]
        async fn test_detect_sends_auth_token() {
            let server = MockServer::start().await;

            Mock::given(method("HEAD"))
                .and(path_regex(r"/api/models/.+"))
                .and(wiremock::matchers::header(
                    "Authorization",
                    "Bearer secret-tok",
                ))
                .respond_with(ResponseTemplate::new(200))
                .mount(&server)
                .await;

            // Without the right token, fall through to 401
            Mock::given(method("HEAD"))
                .respond_with(ResponseTemplate::new(401))
                .mount(&server)
                .await;

            let client = Client::new();

            // With token → finds model
            let result = detect_repo_type_with_base(
                &client,
                &server.uri(),
                "org/gated-model",
                "main",
                Some("secret-tok"),
            )
            .await
            .unwrap();
            assert_eq!(result, RepoType::Model);

            // Without token → all 401
            let result =
                detect_repo_type_with_base(&client, &server.uri(), "org/gated-model", "main", None)
                    .await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_detect_model_wins_when_both_model_and_space_exist() {
            let server = MockServer::start().await;

            // Both model and space return 200 — model should win (tried first)
            Mock::given(method("HEAD"))
                .and(path_regex(r"/api/models/.+"))
                .respond_with(ResponseTemplate::new(200))
                .mount(&server)
                .await;

            Mock::given(method("HEAD"))
                .and(path_regex(r"/api/spaces/.+"))
                .respond_with(ResponseTemplate::new(200))
                .mount(&server)
                .await;

            let client = Client::new();
            let result =
                detect_repo_type_with_base(&client, &server.uri(), "org/ambiguous", "main", None)
                    .await
                    .unwrap();

            assert_eq!(result, RepoType::Model);
        }
    }

    mod error_messages {
        use super::*;
        use wiremock::matchers::path_regex;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        const LIST_PATH: &str = r"/api/models/.+";
        const DL_PATH: &str = r"/.+/resolve/main/config.json";

        #[tokio::test]
        async fn test_list_401_suggests_token_and_type_prefix() {
            let server = MockServer::start().await;
            Mock::given(path_regex(LIST_PATH))
                .respond_with(ResponseTemplate::new(401))
                .mount(&server)
                .await;

            let client = Client::new();
            let err = list_repo_files_with_base(&client, &server.uri(), &model_repo(), None)
                .await
                .unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("HF_TOKEN"), "got: {msg}");
            assert!(msg.contains("401"), "got: {msg}");
            assert!(msg.contains("datasets/"), "got: {msg}");
            assert!(msg.contains("meta-llama/Llama-2-7b"), "got: {msg}");
        }

        #[tokio::test]
        async fn test_list_403_suggests_token() {
            let server = MockServer::start().await;
            Mock::given(path_regex(LIST_PATH))
                .respond_with(ResponseTemplate::new(403))
                .mount(&server)
                .await;

            let client = Client::new();
            let err = list_repo_files_with_base(&client, &server.uri(), &model_repo(), Some("tok"))
                .await
                .unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("403"), "got: {msg}");
            assert!(msg.contains("HF_TOKEN"), "got: {msg}");
        }

        #[tokio::test]
        async fn test_list_404_reports_not_found() {
            let server = MockServer::start().await;
            Mock::given(path_regex(LIST_PATH))
                .respond_with(ResponseTemplate::new(404))
                .mount(&server)
                .await;

            let client = Client::new();
            let err = list_repo_files_with_base(&client, &server.uri(), &model_repo(), None)
                .await
                .unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("not found"), "got: {msg}");
            assert!(msg.contains("meta-llama/Llama-2-7b"), "got: {msg}");
            assert!(!msg.contains("HF_TOKEN"), "got: {msg}");
        }

        #[tokio::test]
        async fn test_list_500_keeps_status_text() {
            let server = MockServer::start().await;
            Mock::given(path_regex(LIST_PATH))
                .respond_with(ResponseTemplate::new(500))
                .mount(&server)
                .await;

            let client = Client::new();
            let err = list_repo_files_with_base(&client, &server.uri(), &model_repo(), None)
                .await
                .unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("500"), "got: {msg}");
            assert!(!msg.contains("HF_TOKEN"), "got: {msg}");
        }

        #[tokio::test]
        async fn test_download_401_suggests_token() {
            let server = MockServer::start().await;
            Mock::given(path_regex(DL_PATH))
                .respond_with(ResponseTemplate::new(401))
                .mount(&server)
                .await;

            let client = Client::new();
            let err = match download_file_stream_with_base(
                &client,
                &server.uri(),
                &model_repo(),
                "config.json",
                None,
                0,
            )
            .await
            {
                Err(e) => e,
                Ok(_) => panic!("expected error"),
            };
            let msg = err.to_string();
            assert!(msg.contains("401"), "got: {msg}");
            assert!(msg.contains("HF_TOKEN"), "got: {msg}");
        }

        #[tokio::test]
        async fn test_download_404_reports_not_found() {
            let server = MockServer::start().await;
            Mock::given(path_regex(DL_PATH))
                .respond_with(ResponseTemplate::new(404))
                .mount(&server)
                .await;

            let client = Client::new();
            let err = match download_file_stream_with_base(
                &client,
                &server.uri(),
                &model_repo(),
                "config.json",
                None,
                0,
            )
            .await
            {
                Err(e) => e,
                Ok(_) => panic!("expected error"),
            };
            let msg = err.to_string();
            assert!(msg.contains("not found"), "got: {msg}");
        }
    }

    mod range_downloads {
        use super::*;
        use futures::StreamExt;
        use wiremock::matchers::header;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        const DL_PATH: &str = r"/.+/resolve/main/config.json";

        #[tokio::test]
        async fn test_range_download_sends_range_header_and_streams_body() {
            let server = MockServer::start().await;
            Mock::given(header("Range", "bytes=8-"))
                .and(wiremock::matchers::path_regex(DL_PATH))
                .respond_with(ResponseTemplate::new(206).set_body_string("tail-bytes"))
                .mount(&server)
                .await;

            let client = Client::new();
            let (mut stream, _len) = match download_file_stream_with_base(
                &client,
                &server.uri(),
                &model_repo(),
                "config.json",
                None,
                8,
            )
            .await
            {
                Ok(v) => v,
                Err(e) => panic!("range download should succeed: {e}"),
            };

            let mut body = String::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.expect("chunk");
                body.push_str(std::str::from_utf8(&chunk).expect("utf8"));
            }
            assert_eq!(body, "tail-bytes");
        }

        #[tokio::test]
        async fn test_range_download_rejects_server_that_ignores_range() {
            let server = MockServer::start().await;
            // Server ignores the Range header and returns the whole file
            Mock::given(wiremock::matchers::path_regex(DL_PATH))
                .respond_with(ResponseTemplate::new(200).set_body_string("whole-file"))
                .mount(&server)
                .await;

            let client = Client::new();
            let err = match download_file_stream_with_base(
                &client,
                &server.uri(),
                &model_repo(),
                "config.json",
                None,
                8,
            )
            .await
            {
                Err(e) => e,
                Ok(_) => panic!("must not resume from a full-body response"),
            };
            let msg = err.to_string();
            assert!(msg.contains("206"), "got: {msg}");
        }
    }
}
