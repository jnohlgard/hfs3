use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// HuggingFace repo type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepoType {
    Model,
    Dataset,
    Space,
}

impl fmt::Display for RepoType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RepoType::Model => write!(f, "model"),
            RepoType::Dataset => write!(f, "dataset"),
            RepoType::Space => write!(f, "space"),
        }
    }
}

impl FromStr for RepoType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "model" => Ok(RepoType::Model),
            "dataset" => Ok(RepoType::Dataset),
            "space" => Ok(RepoType::Space),
            other => Err(format!("unknown repo type: {other}")),
        }
    }
}

/// Parsed HuggingFace repo reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoRef {
    /// e.g. "meta-llama/Llama-2-7b"
    pub repo_id: String,
    /// Model, Dataset, Space
    pub repo_type: RepoType,
    /// git revision, default "main"
    pub revision: String,
}

/// A single file entry from the HF tree API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HfFileEntry {
    /// relative path within repo
    pub path: String,
    /// file size in bytes
    pub size: u64,
    /// git object ID (sha256)
    pub oid: String,
}

use crate::stats::TransferStatsReport;

/// Result of a mirror operation, serialized as JSON to stdout.
#[derive(Debug, Serialize)]
pub struct MirrorResult {
    pub repo_id: String,
    pub repo_type: String,
    pub bucket: String,
    pub prefix: String,
    pub files_transferred: usize,
    pub files_failed: usize,
    pub bytes_transferred: u64,
    pub failed_files: Vec<String>,
    pub duration_secs: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<TransferStatsReport>,
}

/// Result of a pull operation, serialized as JSON to stdout.
#[derive(Debug, Serialize)]
pub struct PullResult {
    pub repo_id: String,
    pub repo_type: String,
    pub dest: PathBuf,
    pub files_downloaded: usize,
    pub bytes_downloaded: u64,
    pub duration_secs: f64,
}

/// Result of a run operation, serialized as JSON to stdout.
#[derive(Debug, Serialize)]
pub struct RunResult {
    pub repo_id: String,
    pub repo_type: String,
    pub image_tag: String,
    pub port: u16,
}

/// Parse a HF URL or bare repo ID into a RepoRef.
///
/// Accepts:
///   - "meta-llama/Llama-2-7b"                         -> Model
///   - "https://huggingface.co/meta-llama/Llama-2-7b"  -> Model
///   - "https://huggingface.co/spaces/user/my-space"   -> Space
///   - "https://huggingface.co/datasets/user/my-ds"    -> Dataset
///   - URLs with /tree/revision suffix
pub fn parse_repo_url(url_or_id: &str) -> Result<RepoRef, crate::error::Hfs3Error> {
    // 1. Trim whitespace and trailing slash
    let trimmed = url_or_id.trim().trim_end_matches('/');

    // 2. Strip https://huggingface.co/ prefix if present
    let path = if let Some(rest) = trimmed.strip_prefix("https://huggingface.co/") {
        rest.to_string()
    } else if let Some(rest) = trimmed.strip_prefix("http://huggingface.co/") {
        rest.to_string()
    } else {
        trimmed.to_string()
    };

    // 3. Detect repo_type from path prefix (spaces/, datasets/)
    let (repo_type, remainder) = if let Some(rest) = path.strip_prefix("spaces/") {
        (RepoType::Space, rest.to_string())
    } else if let Some(rest) = path.strip_prefix("datasets/") {
        (RepoType::Dataset, rest.to_string())
    } else {
        (RepoType::Model, path)
    };

    // 4. Extract revision from /tree/xxx suffix
    let (id_part, revision) = if let Some(tree_idx) = remainder.find("/tree/") {
        let id = &remainder[..tree_idx];
        let rev = &remainder[tree_idx + 6..]; // skip "/tree/"
        (id.to_string(), rev.to_string())
    } else {
        (remainder, "main".to_string())
    };

    // 5. Validate repo_id contains a slash (owner/name)
    if !id_part.contains('/') {
        return Err(crate::error::Hfs3Error::Parse(format!(
            "Invalid repo ID: '{}' — expected 'owner/name' format",
            id_part
        )));
    }

    Ok(RepoRef {
        repo_id: id_part,
        repo_type,
        revision,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Tests ported from Python tests/test_hf.py ----

    #[test]
    fn test_bare_model_id() {
        let ref_ = parse_repo_url("meta-llama/Llama-2-7b").unwrap();
        assert_eq!(ref_.repo_id, "meta-llama/Llama-2-7b");
        assert_eq!(ref_.repo_type, RepoType::Model);
        assert_eq!(ref_.revision, "main");
    }

    #[test]
    fn test_model_url() {
        let ref_ = parse_repo_url("https://huggingface.co/meta-llama/Llama-2-7b").unwrap();
        assert_eq!(ref_.repo_id, "meta-llama/Llama-2-7b");
        assert_eq!(ref_.repo_type, RepoType::Model);
        assert_eq!(ref_.revision, "main");
    }

    #[test]
    fn test_space_url() {
        let ref_ = parse_repo_url("https://huggingface.co/spaces/user/my-space").unwrap();
        assert_eq!(ref_.repo_id, "user/my-space");
        assert_eq!(ref_.repo_type, RepoType::Space);
        assert_eq!(ref_.revision, "main");
    }

    #[test]
    fn test_dataset_url() {
        let ref_ = parse_repo_url("https://huggingface.co/datasets/user/my-ds").unwrap();
        assert_eq!(ref_.repo_id, "user/my-ds");
        assert_eq!(ref_.repo_type, RepoType::Dataset);
        assert_eq!(ref_.revision, "main");
    }

    #[test]
    fn test_url_with_revision() {
        let ref_ = parse_repo_url("https://huggingface.co/meta-llama/Llama-2-7b/tree/dev").unwrap();
        assert_eq!(ref_.repo_id, "meta-llama/Llama-2-7b");
        assert_eq!(ref_.repo_type, RepoType::Model);
        assert_eq!(ref_.revision, "dev");
    }

    #[test]
    fn test_trailing_slash_stripped() {
        let ref_ = parse_repo_url("https://huggingface.co/spaces/user/my-space/").unwrap();
        assert_eq!(ref_.repo_id, "user/my-space");
    }

    #[test]
    fn test_whitespace_stripped() {
        let ref_ = parse_repo_url("  meta-llama/Llama-2-7b  ").unwrap();
        assert_eq!(ref_.repo_id, "meta-llama/Llama-2-7b");
    }

    #[test]
    fn test_invalid_no_slash() {
        assert!(parse_repo_url("just-a-name").is_err());
    }

    #[test]
    fn test_invalid_bare_namespace() {
        assert!(parse_repo_url("https://huggingface.co/spaces/onlynamespace").is_err());
    }

    // ---- Additional tests for RepoType Display/FromStr ----

    #[test]
    fn test_repo_type_display() {
        assert_eq!(RepoType::Model.to_string(), "model");
        assert_eq!(RepoType::Dataset.to_string(), "dataset");
        assert_eq!(RepoType::Space.to_string(), "space");
    }

    #[test]
    fn test_repo_type_from_str() {
        assert_eq!(RepoType::from_str("model").unwrap(), RepoType::Model);
        assert_eq!(RepoType::from_str("dataset").unwrap(), RepoType::Dataset);
        assert_eq!(RepoType::from_str("space").unwrap(), RepoType::Space);
    }

    #[test]
    fn test_repo_type_from_str_invalid() {
        assert!(RepoType::from_str("unknown").is_err());
    }

    #[test]
    fn test_repo_type_roundtrip() {
        for rt in [RepoType::Model, RepoType::Dataset, RepoType::Space] {
            let s = rt.to_string();
            let parsed = RepoType::from_str(&s).unwrap();
            assert_eq!(parsed, rt);
        }
    }
}
