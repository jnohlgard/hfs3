use bytes::Bytes;
use futures::Stream;
use reqwest::Client;

use crate::error::Hfs3Error;
use crate::types::{HfFileEntry, RepoRef, RepoType};

const HF_BASE: &str = "https://huggingface.co";

/// Build the HF tree API URL for listing repo files.
fn api_url(repo: &RepoRef) -> String {
    api_url_with_base(HF_BASE, repo)
}

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
fn download_url(repo: &RepoRef, file_path: &str) -> String {
    let type_prefix = match repo.repo_type {
        RepoType::Model => String::new(),
        RepoType::Dataset => "datasets/".to_string(),
        RepoType::Space => "spaces/".to_string(),
    };
    format!(
        "{}/{}{}/resolve/{}/{}",
        HF_BASE, type_prefix, repo.repo_id, repo.revision, file_path
    )
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

async fn detect_repo_type_with_base(
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
    let url = api_url(repo);

    let mut req = client.get(&url);
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }

    let resp = req.send().await?;

    if !resp.status().is_success() {
        return Err(Hfs3Error::HfApi(format!(
            "GET {} returned {}",
            url,
            resp.status()
        )));
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
    let url = download_url(repo, file_path);

    let mut req = client.get(&url);
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }

    let resp = req.send().await?;

    if !resp.status().is_success() {
        return Err(Hfs3Error::HfApi(format!(
            "GET {} returned {}",
            url,
            resp.status()
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
            api_url(&repo),
            "https://huggingface.co/api/models/meta-llama/Llama-2-7b/tree/main?recursive=true"
        );
    }

    #[test]
    fn test_api_url_dataset() {
        let repo = dataset_repo();
        assert_eq!(
            api_url(&repo),
            "https://huggingface.co/api/datasets/user/my-dataset/tree/main?recursive=true"
        );
    }

    #[test]
    fn test_download_url_model() {
        let repo = model_repo();
        assert_eq!(
            download_url(&repo, "config.json"),
            "https://huggingface.co/meta-llama/Llama-2-7b/resolve/main/config.json"
        );
    }

    #[test]
    fn test_download_url_space() {
        let repo = space_repo();
        assert_eq!(
            download_url(&repo, "app.py"),
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
        async fn test_detect_space_after_model_404() {
            let server = MockServer::start().await;

            // Model → 404
            Mock::given(method("HEAD"))
                .and(path_regex(r"/api/models/.+"))
                .respond_with(ResponseTemplate::new(404))
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
        async fn test_detect_dataset_after_model_and_space_404() {
            let server = MockServer::start().await;

            Mock::given(method("HEAD"))
                .and(path_regex(r"/api/models/.+"))
                .respond_with(ResponseTemplate::new(404))
                .mount(&server)
                .await;

            Mock::given(method("HEAD"))
                .and(path_regex(r"/api/spaces/.+"))
                .respond_with(ResponseTemplate::new(404))
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
        async fn test_detect_all_404_returns_error() {
            let server = MockServer::start().await;

            Mock::given(method("HEAD"))
                .respond_with(ResponseTemplate::new(404))
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

            // Without the right token, fall through to 404
            Mock::given(method("HEAD"))
                .respond_with(ResponseTemplate::new(404))
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

            // Without token → all 404
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
}
