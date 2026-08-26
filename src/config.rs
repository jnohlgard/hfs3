use crate::error::Hfs3Error;

/// Runtime configuration. All fields sourced from environment variables.
#[derive(Debug, Clone)]
pub struct AppConfig {
    /// S3 bucket for mirrored repos (required).
    pub s3_bucket: String,
    /// Key prefix within the bucket. Default: "hfs3-mirror".
    pub s3_prefix: String,
    /// HuggingFace auth token for gated repos (optional).
    pub hf_token: Option<String>,
    /// AWS region for S3 client (optional, uses SDK default if unset).
    pub aws_region: Option<String>,
    /// S3 endpoint URL override (optional; e.g. a local MinIO for testing).
    pub s3_endpoint: Option<String>,
}

impl AppConfig {
    /// Build config from environment variables.
    ///
    /// Required: HFS3_S3_BUCKET
    /// Optional: HFS3_S3_PREFIX (default "hfs3-mirror"), HF_TOKEN, AWS_REGION,
    /// HFS3_S3_ENDPOINT
    pub fn from_env() -> Result<Self, Hfs3Error> {
        let s3_bucket = std::env::var("HFS3_S3_BUCKET")
            .map_err(|_| Hfs3Error::Config("HFS3_S3_BUCKET is required".into()))?;

        Ok(Self {
            s3_bucket,
            s3_prefix: std::env::var("HFS3_S3_PREFIX").unwrap_or_else(|_| "hfs3-mirror".into()),
            hf_token: std::env::var("HF_TOKEN").ok(),
            aws_region: std::env::var("AWS_REGION").ok(),
            s3_endpoint: std::env::var("HFS3_S3_ENDPOINT").ok(),
        })
    }

    /// S3 key prefix for a given repo ref.
    /// Format: {s3_prefix}/{repo_type}/{owner}--{name}
    pub fn s3_prefix_for(&self, repo_type: &str, repo_id: &str) -> String {
        format!(
            "{}/{}/{}",
            self.s3_prefix,
            repo_type,
            repo_id.replace('/', "--")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::sync::Mutex;

    // Config tests mutate environment variables. Rust runs tests in parallel
    // by default, so we use a static mutex to ensure only one test modifies
    // the environment at a time. Run with `--test-threads=1` for extra safety,
    // but the mutex guarantees correctness regardless.
    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    /// Helper: run test with specific env vars set, restore after.
    #[allow(unused_unsafe)] // set_var/remove_var are unsafe in Rust >= 1.83, safe before
    fn with_env<F: FnOnce()>(vars: &[(&str, Option<&str>)], f: F) {
        let _lock = ENV_MUTEX.lock().unwrap();
        // Save originals
        let originals: Vec<_> = vars.iter().map(|(k, _)| (*k, env::var(k).ok())).collect();
        // Set/unset
        for (k, v) in vars {
            match v {
                Some(val) => unsafe { env::set_var(k, val) },
                None => unsafe { env::remove_var(k) },
            }
        }
        f();
        // Restore
        for (k, orig) in originals {
            match orig {
                Some(val) => unsafe { env::set_var(k, val) },
                None => unsafe { env::remove_var(k) },
            }
        }
    }

    #[test]
    fn test_from_env_minimal() {
        with_env(
            &[
                ("HFS3_S3_BUCKET", Some("my-bucket")),
                ("HFS3_S3_PREFIX", None),
                ("HF_TOKEN", None),
                ("AWS_REGION", None),
                ("HFS3_S3_ENDPOINT", None),
            ],
            || {
                let cfg = AppConfig::from_env().unwrap();
                assert_eq!(cfg.s3_bucket, "my-bucket");
                assert_eq!(cfg.s3_prefix, "hfs3-mirror");
                assert!(cfg.hf_token.is_none());
                assert!(cfg.s3_endpoint.is_none());
            },
        );
    }

    #[test]
    fn test_from_env_full() {
        with_env(
            &[
                ("HFS3_S3_BUCKET", Some("prod-bucket")),
                ("HFS3_S3_PREFIX", Some("custom/prefix")),
                ("HF_TOKEN", Some("hf_abc123")),
                ("AWS_REGION", Some("us-west-2")),
                ("HFS3_S3_ENDPOINT", Some("http://localhost:9000")),
            ],
            || {
                let cfg = AppConfig::from_env().unwrap();
                assert_eq!(cfg.s3_bucket, "prod-bucket");
                assert_eq!(cfg.s3_prefix, "custom/prefix");
                assert_eq!(cfg.hf_token.as_deref(), Some("hf_abc123"));
                assert_eq!(cfg.aws_region.as_deref(), Some("us-west-2"));
                assert_eq!(cfg.s3_endpoint.as_deref(), Some("http://localhost:9000"));
            },
        );
    }

    #[test]
    fn test_missing_bucket_raises() {
        with_env(&[("HFS3_S3_BUCKET", None)], || {
            let err = AppConfig::from_env().unwrap_err();
            assert!(err.to_string().contains("HFS3_S3_BUCKET"));
        });
    }

    #[test]
    fn test_s3_prefix_for() {
        let cfg = AppConfig {
            s3_bucket: "b".into(),
            s3_prefix: "hfs3-mirror".into(),
            hf_token: None,
            aws_region: None,
            s3_endpoint: None,
        };
        assert_eq!(
            cfg.s3_prefix_for("model", "meta-llama/Llama-2-7b"),
            "hfs3-mirror/model/meta-llama--Llama-2-7b"
        );
    }
}
