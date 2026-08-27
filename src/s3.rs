//! S3 operations for hfs3: multipart upload, download, and listing.
//!
//! Uses aws-sdk-s3 for all S3 interactions. Supports streaming multipart
//! uploads (zero-copy from HF download stream) and adaptive chunk sizing.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client as S3Client;
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::concurrency::{plan_transfer, plan_transfer_with_memory};
use crate::error::Hfs3Error;

// Re-export chunk functions so consumers can use them via s3 module.
pub use crate::concurrency::{chunk_size_for_file, chunk_size_for_transfer};

/// Join a relative key path to the destination directory, rejecting any
/// path that would escape it (absolute paths, `..` components).
pub fn safe_join(dest_dir: &Path, relative: &str) -> Result<PathBuf, Hfs3Error> {
    let rel = Path::new(relative);
    if rel.is_absolute() {
        return Err(Hfs3Error::S3(format!(
            "refusing unsafe key path (absolute): {relative}"
        )));
    }
    if rel.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(Hfs3Error::S3(format!(
            "refusing unsafe key path (contains ..): {relative}"
        )));
    }
    let joined = dest_dir.join(rel);
    if !joined.starts_with(dest_dir) {
        return Err(Hfs3Error::S3(format!(
            "refusing unsafe key path (escapes destination): {relative}"
        )));
    }
    Ok(joined)
}

/// Parameters controlling how a file is uploaded to S3.
#[derive(Debug, Clone)]
pub struct UploadParams {
    /// Bytes per S3 multipart part.
    pub chunk_size: usize,
    /// Max S3 parts uploading concurrently (1 = sequential, legacy behavior).
    pub max_parts_in_flight: usize,
}

impl UploadParams {
    /// Default params based only on file size (no memory awareness).
    pub fn for_file(file_size: u64) -> Self {
        Self {
            chunk_size: chunk_size_for_file(file_size),
            max_parts_in_flight: 1,
        }
    }
}

/// Threshold below which we use put_object instead of multipart upload.
const PUT_OBJECT_THRESHOLD: u64 = 8 * 1024 * 1024; // 8 MB

/// S3 operations wrapper around aws-sdk-s3 client.
#[derive(Clone)]
pub struct S3Ops {
    client: S3Client,
}

impl S3Ops {
    /// Create a new S3Ops from AWS config, with optional region and
    /// endpoint overrides (endpoint for S3-compatible servers such as
    /// a local MinIO).
    pub async fn new(region: Option<&str>, endpoint: Option<&str>) -> Result<Self, Hfs3Error> {
        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(r) = region {
            config_loader = config_loader.region(aws_config::Region::new(r.to_owned()));
        }
        let endpoint_set = endpoint.is_some();
        if let Some(e) = endpoint {
            config_loader = config_loader.endpoint_url(e.to_owned());
        }
        let sdk_config = config_loader.load().await;
        // Custom endpoints (e.g. cluster S3, MinIO) generally don't resolve
        // virtual-host subdomains, so force path-style bucket addressing.
        let s3_config = if endpoint_set {
            aws_sdk_s3::config::Builder::from(&sdk_config)
                .force_path_style(true)
                .build()
        } else {
            aws_sdk_s3::config::Builder::from(&sdk_config).build()
        };
        let client = S3Client::from_conf(s3_config);
        Ok(Self { client })
    }

    /// Create S3Ops from an existing client (useful for testing).
    pub fn from_client(client: S3Client) -> Self {
        Self { client }
    }

    /// Upload a byte stream to S3 using put_object (small files) or multipart upload (large files).
    ///
    /// Uses default params (file-size-based chunks, sequential parts).
    /// Returns total bytes uploaded.
    pub async fn upload_multipart_stream(
        &self,
        bucket: &str,
        key: &str,
        stream: impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
        file_size: u64,
    ) -> Result<u64, Hfs3Error> {
        let params = UploadParams::for_file(file_size);
        self.upload_multipart_stream_with_progress(bucket, key, stream, file_size, &params, |_| {})
            .await
    }

    /// Upload a byte stream with a per-part progress callback and tunable params.
    ///
    /// `on_part_uploaded` is called with the byte count after each S3 part
    /// (or put_object for small files) completes successfully.
    pub async fn upload_multipart_stream_with_progress<F>(
        &self,
        bucket: &str,
        key: &str,
        stream: impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
        file_size: u64,
        params: &UploadParams,
        on_part_uploaded: F,
    ) -> Result<u64, Hfs3Error>
    where
        F: Fn(u64),
    {
        if file_size < PUT_OBJECT_THRESHOLD {
            let bytes = self.upload_small(bucket, key, stream, file_size).await?;
            on_part_uploaded(bytes);
            Ok(bytes)
        } else {
            self.upload_multipart(bucket, key, stream, file_size, params, on_part_uploaded)
                .await
        }
    }

    /// Upload a small file using put_object (collect entire body first).
    ///
    /// Rejects the stream if its actual byte count differs from the listed
    /// `file_size`, so a truncated download never lands in S3.
    async fn upload_small(
        &self,
        bucket: &str,
        key: &str,
        mut stream: impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
        file_size: u64,
    ) -> Result<u64, Hfs3Error> {
        let mut buf = BytesMut::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(Hfs3Error::Http)?;
            buf.extend_from_slice(&chunk);
        }
        let total = buf.len() as u64;
        if total != file_size {
            return Err(Hfs3Error::S3(format!(
                "size mismatch for {key}: listed {file_size} bytes, stream ended at {total}"
            )));
        }
        let body = ByteStream::from(buf.freeze());

        tracing::info!(bucket, key, bytes = total, "put_object (small file)");

        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            .send()
            .await
            .map_err(|e| Hfs3Error::S3(format!("put_object failed for {key}: {e}")))?;

        Ok(total)
    }

    /// Upload a large file using multipart upload with concurrent part uploads.
    ///
    /// Parts are buffered from the stream and uploaded via a JoinSet,
    /// bounded by `params.max_parts_in_flight`. On error, all in-flight
    /// uploads are aborted before the multipart upload is cancelled.
    async fn upload_multipart<F>(
        &self,
        bucket: &str,
        key: &str,
        mut stream: impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
        file_size: u64,
        params: &UploadParams,
        on_part_uploaded: F,
    ) -> Result<u64, Hfs3Error>
    where
        F: Fn(u64),
    {
        let chunk_size = params.chunk_size;
        let max_in_flight = params.max_parts_in_flight;

        // Create multipart upload
        let create_resp = self
            .client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| Hfs3Error::S3(format!("create_multipart_upload failed for {key}: {e}")))?;

        let upload_id = create_resp
            .upload_id()
            .ok_or_else(|| Hfs3Error::S3("no upload_id returned".into()))?
            .to_string();

        let mut completed_parts: Vec<(i32, CompletedPart)> = Vec::new();
        let mut part_number: i32 = 1;
        let mut total_bytes: u64 = 0;
        let mut buf = BytesMut::with_capacity(chunk_size);
        let mut in_flight: JoinSet<Result<(i32, CompletedPart, u64), Hfs3Error>> = JoinSet::new();

        let result: Result<(), Hfs3Error> = async {
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(Hfs3Error::Http)?;
                buf.extend_from_slice(&chunk);

                // Eagerly collect any completed uploads (non-blocking).
                // This keeps progress reporting responsive and frees memory
                // from finished parts without waiting for backpressure.
                while let Some(join_result) = in_flight.try_join_next() {
                    let (pnum, part, bytes) = join_result
                        .map_err(|e| Hfs3Error::S3(format!("part upload task panicked: {e}")))??;
                    completed_parts.push((pnum, part));
                    total_bytes += bytes;
                    on_part_uploaded(bytes);
                }

                while buf.len() >= chunk_size {
                    // If at capacity, wait for one in-flight part to complete
                    while in_flight.len() >= max_in_flight {
                        if let Some(join_result) = in_flight.join_next().await {
                            let (pnum, part, bytes) = join_result.map_err(|e| {
                                Hfs3Error::S3(format!("part upload task panicked: {e}"))
                            })??;
                            completed_parts.push((pnum, part));
                            total_bytes += bytes;
                            on_part_uploaded(bytes);
                        }
                    }

                    let part_data = buf.split_to(chunk_size).freeze();
                    let part_len = part_data.len() as u64;

                    // Spawn concurrent part upload
                    let client = self.client.clone();
                    let b = bucket.to_string();
                    let k = key.to_string();
                    let uid = upload_id.clone();
                    let pn = part_number;

                    in_flight.spawn(async move {
                        let body = ByteStream::from(part_data);
                        let resp = client
                            .upload_part()
                            .bucket(&b)
                            .key(&k)
                            .upload_id(&uid)
                            .part_number(pn)
                            .content_length(part_len as i64)
                            .body(body)
                            .send()
                            .await
                            .map_err(|e| {
                                Hfs3Error::S3(format!("upload_part {pn} failed for {k}: {e}"))
                            })?;

                        let etag = resp
                            .e_tag()
                            .ok_or_else(|| Hfs3Error::S3(format!("no ETag for part {pn}")))?
                            .to_string();

                        let completed =
                            CompletedPart::builder().e_tag(etag).part_number(pn).build();

                        Ok((pn, completed, part_len))
                    });

                    tracing::debug!(
                        part_number,
                        part_bytes = part_len,
                        in_flight = in_flight.len(),
                        "enqueued part upload"
                    );
                    part_number += 1;
                }
            }

            // Upload remaining bytes as final part
            if !buf.is_empty() {
                let part_data = buf.freeze();
                let part_len = part_data.len() as u64;

                let client = self.client.clone();
                let b = bucket.to_string();
                let k = key.to_string();
                let uid = upload_id.clone();
                let pn = part_number;

                in_flight.spawn(async move {
                    let body = ByteStream::from(part_data);
                    let resp = client
                        .upload_part()
                        .bucket(&b)
                        .key(&k)
                        .upload_id(&uid)
                        .part_number(pn)
                        .content_length(part_len as i64)
                        .body(body)
                        .send()
                        .await
                        .map_err(|e| {
                            Hfs3Error::S3(format!("upload_part {pn} failed for {k}: {e}"))
                        })?;

                    let etag = resp
                        .e_tag()
                        .ok_or_else(|| Hfs3Error::S3(format!("no ETag for part {pn}")))?
                        .to_string();

                    let completed = CompletedPart::builder().e_tag(etag).part_number(pn).build();

                    Ok((pn, completed, part_len))
                });
                part_number += 1;
            }

            // Drain all remaining in-flight parts
            while let Some(result) = in_flight.join_next().await {
                let (pnum, part, bytes) = result
                    .map_err(|e| Hfs3Error::S3(format!("part upload task panicked: {e}")))??;
                completed_parts.push((pnum, part));
                total_bytes += bytes;
                on_part_uploaded(bytes);
            }

            // Reject a stream whose actual size differs from the listed size
            // so a truncated download never lands in S3.
            if total_bytes != file_size {
                return Err(Hfs3Error::S3(format!(
                    "size mismatch for {key}: listed {file_size} bytes, uploaded {total_bytes}"
                )));
            }

            Ok(())
        }
        .await;

        // On failure, abort all in-flight tasks then abort the multipart upload
        if let Err(e) = result {
            in_flight.abort_all();
            // Drain to ensure all tasks are cleaned up
            while in_flight.join_next().await.is_some() {}

            tracing::warn!(
                bucket,
                key,
                upload_id = %upload_id,
                "aborting multipart upload due to error"
            );
            let _ = self
                .client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .send()
                .await;
            return Err(e);
        }

        // Sort by part number (parts may complete out of order) and validate contiguity
        completed_parts.sort_by_key(|(pnum, _)| *pnum);
        let expected_count = (part_number - 1) as usize;
        if completed_parts.len() != expected_count {
            let _ = self
                .client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .send()
                .await;
            return Err(Hfs3Error::S3(format!(
                "part count mismatch for {key}: expected {expected_count}, got {}",
                completed_parts.len()
            )));
        }

        let parts: Vec<CompletedPart> = completed_parts.into_iter().map(|(_, part)| part).collect();

        // Complete the multipart upload
        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();

        self.client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(|e| {
                Hfs3Error::S3(format!("complete_multipart_upload failed for {key}: {e}"))
            })?;

        tracing::info!(
            bucket,
            key,
            total_bytes,
            parts = part_number - 1,
            max_in_flight,
            chunk_size_mb = chunk_size / (1024 * 1024),
            "multipart upload complete"
        );

        Ok(total_bytes)
    }

    /// Download an object from S3 to a local file.
    ///
    /// Writes to a `<dest>.hfs3-tmp` file and renames it into place, so a
    /// failed download never leaves a partial file at the destination.
    /// Creates parent directories if needed. Returns bytes written.
    pub async fn download_to_file(
        &self,
        bucket: &str,
        key: &str,
        dest: &Path,
    ) -> Result<u64, Hfs3Error> {
        // Create parent directories
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let file_name = dest
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "download".to_string());
        let tmp_path = dest.with_file_name(format!("{file_name}.hfs3-tmp"));

        let write_result: Result<u64, Hfs3Error> = async {
            let resp = self
                .client
                .get_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .map_err(|e| Hfs3Error::S3(format!("get_object failed for {key}: {e}")))?;

            let mut body = resp.body.into_async_read();
            let mut file = tokio::fs::File::create(&tmp_path).await?;
            let bytes_written = tokio::io::copy(&mut body, &mut file).await?;
            file.flush().await?;
            Ok(bytes_written)
        }
        .await;

        match write_result {
            Ok(bytes_written) => {
                if let Err(e) = tokio::fs::rename(&tmp_path, dest).await {
                    let _ = tokio::fs::remove_file(&tmp_path).await;
                    return Err(Hfs3Error::Io(e));
                }
                tracing::info!(
                    bucket,
                    key,
                    dest = %dest.display(),
                    bytes = bytes_written,
                    "downloaded to file"
                );
                Ok(bytes_written)
            }
            Err(e) => {
                let _ = tokio::fs::remove_file(&tmp_path).await;
                Err(e)
            }
        }
    }

    /// List all objects under a prefix, handling pagination.
    ///
    /// Returns a list of (key, size) tuples.
    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<(String, u64)>, Hfs3Error> {
        let mut objects: Vec<(String, u64)> = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = self.client.list_objects_v2().bucket(bucket).prefix(prefix);

            if let Some(token) = continuation_token.take() {
                req = req.continuation_token(token);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| Hfs3Error::S3(format!("list_objects_v2 failed: {e}")))?;

            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    let size = obj.size().unwrap_or(0) as u64;
                    objects.push((key.to_string(), size));
                }
            }

            if resp.is_truncated() == Some(true) {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        tracing::info!(bucket, prefix, count = objects.len(), "listed objects");
        Ok(objects)
    }

    /// Download all objects under a prefix to a local directory.
    ///
    /// Strips the prefix from keys to form local paths, and downloads files
    /// concurrently with a memory-aware bound (same model as mirror).
    /// Returns (files_downloaded, total_bytes).
    pub async fn download_all(
        &self,
        bucket: &str,
        prefix: &str,
        dest_dir: &Path,
    ) -> Result<(usize, u64), Hfs3Error> {
        let objects = self.list_objects(bucket, prefix).await?;
        let mut files_downloaded: usize = 0;
        let mut files_skipped: usize = 0;
        let mut total_bytes: u64 = 0;

        // Normalize prefix for stripping (ensure trailing slash)
        let strip_prefix = if prefix.ends_with('/') {
            prefix.to_string()
        } else {
            format!("{prefix}/")
        };

        let plan_files: Vec<(&str, u64)> = objects.iter().map(|(k, s)| (k.as_str(), *s)).collect();
        let plan = match plan_transfer(&plan_files).await {
            Ok(p) => p,
            Err(_) => plan_transfer_with_memory(&plan_files, 4 * 1024 * 1024 * 1024),
        };

        let max_concurrent = plan.max_concurrent.max(1).min(plan_files.len().max(1));
        tracing::info!(
            prefix,
            max_concurrent,
            files = plan_files.len(),
            "pull transfer plan"
        );

        let semaphore = Arc::new(Semaphore::new(max_concurrent));
        let mut join_set = JoinSet::new();

        for (key, size) in &objects {
            // Strip the prefix to get relative path
            let relative = key.strip_prefix(&strip_prefix).unwrap_or(key);

            if relative.is_empty() {
                continue;
            }

            let dest_path = safe_join(dest_dir, relative)?;

            // Skip files that are already downloaded with a matching size
            if let Ok(meta) = tokio::fs::metadata(&dest_path).await {
                if meta.is_file() && meta.len() == *size {
                    files_skipped += 1;
                    tracing::info!(key, "skipping existing file (size match)");
                    continue;
                }
            }

            let s3 = self.clone();
            let bucket = bucket.to_string();
            let key = key.clone();
            let sem = semaphore.clone();

            join_set.spawn(async move {
                let _permit = sem.acquire_owned().await.expect("semaphore not closed");
                s3.download_to_file(&bucket, &key, &dest_path).await
            });
        }

        while let Some(result) = join_set.join_next().await {
            let bytes = match result {
                Ok(Ok(bytes)) => bytes,
                Ok(Err(e)) => return Err(e),
                Err(e) => {
                    return Err(Hfs3Error::Io(std::io::Error::other(format!(
                        "download task panicked: {e}"
                    ))))
                }
            };
            files_downloaded += 1;
            total_bytes += bytes;
        }

        tracing::info!(
            bucket,
            prefix,
            files = files_downloaded,
            skipped = files_skipped,
            bytes = total_bytes,
            dest = %dest_dir.display(),
            "download_all complete"
        );

        if files_skipped > 0 {
            eprintln!("Skipped {files_skipped} existing file(s) with matching size");
        }

        Ok((files_downloaded, total_bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MB: usize = 1024 * 1024;
    const GB: u64 = 1024 * 1024 * 1024;

    #[test]
    fn test_chunk_size_tiny_file() {
        assert_eq!(chunk_size_for_file(1), 8 * MB);
    }

    #[test]
    fn test_chunk_size_zero() {
        assert_eq!(chunk_size_for_file(0), 8 * MB);
    }

    #[test]
    fn test_chunk_size_small_file() {
        assert_eq!(chunk_size_for_file(500 * 1024 * 1024), 8 * MB);
    }

    #[test]
    fn test_chunk_size_boundary_below_1gb() {
        assert_eq!(chunk_size_for_file(GB - 1), 8 * MB);
    }

    #[test]
    fn test_chunk_size_boundary_at_1gb() {
        assert_eq!(chunk_size_for_file(GB), 64 * MB);
    }

    #[test]
    fn test_chunk_size_medium_file() {
        assert_eq!(chunk_size_for_file(2 * GB), 64 * MB);
    }

    #[test]
    fn test_chunk_size_boundary_below_5gb() {
        assert_eq!(chunk_size_for_file(5 * GB - 1), 64 * MB);
    }

    #[test]
    fn test_chunk_size_boundary_at_5gb() {
        assert_eq!(chunk_size_for_file(5 * GB), 128 * MB);
    }

    #[test]
    fn test_chunk_size_large_file() {
        assert_eq!(chunk_size_for_file(10 * GB), 128 * MB);
    }

    #[test]
    fn test_chunk_size_very_large_file() {
        assert_eq!(chunk_size_for_file(100 * GB), 128 * MB);
    }

    #[test]
    fn test_safe_join_nest() {
        let path =
            safe_join(Path::new("/data/repo"), "onnx/model.onnx").expect("nested path is safe");
        assert_eq!(path, Path::new("/data/repo/onnx/model.onnx"));
    }

    #[test]
    fn test_safe_join_top_level() {
        let path =
            safe_join(Path::new("/data/repo"), "config.json").expect("top-level path is safe");
        assert_eq!(path, Path::new("/data/repo/config.json"));
    }

    #[test]
    fn test_safe_join_rejects_absolute() {
        let err = safe_join(Path::new("/data/repo"), "/etc/passwd").unwrap_err();
        assert!(err.to_string().contains("absolute"));
    }

    #[test]
    fn test_safe_join_rejects_parent_traversal() {
        let err = safe_join(Path::new("/data/repo"), "a/../../evil.sh").unwrap_err();
        assert!(err.to_string().contains(".."));
    }

    #[test]
    fn test_safe_join_rejects_bare_parent() {
        let err = safe_join(Path::new("/data/repo"), "..").unwrap_err();
        assert!(err.to_string().contains(".."));
    }

    #[test]
    fn test_safe_join_rejects_leading_parent() {
        let err = safe_join(Path::new("/data/repo"), "../repo2/evil.sh").unwrap_err();
        assert!(err.to_string().contains(".."));
    }
}
