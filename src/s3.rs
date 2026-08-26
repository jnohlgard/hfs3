//! S3 operations for hfs3: multipart upload, download, and listing.
//!
//! Uses aws-sdk-s3 for all S3 interactions. Supports streaming multipart
//! uploads (zero-copy from HF download stream) and adaptive chunk sizing.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client as S3Client;
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use std::path::Path;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;

use crate::error::Hfs3Error;

// Re-export chunk functions so consumers can use them via s3 module.
pub use crate::concurrency::{chunk_size_for_file, chunk_size_for_transfer};

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
        if let Some(e) = endpoint {
            config_loader = config_loader.endpoint_url(e.to_owned());
        }
        let sdk_config = config_loader.load().await;
        let client = S3Client::new(&sdk_config);
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
            let bytes = self.upload_small(bucket, key, stream).await?;
            on_part_uploaded(bytes);
            Ok(bytes)
        } else {
            self.upload_multipart(bucket, key, stream, file_size, params, on_part_uploaded)
                .await
        }
    }

    /// Upload a small file using put_object (collect entire body first).
    async fn upload_small(
        &self,
        bucket: &str,
        key: &str,
        mut stream: impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
    ) -> Result<u64, Hfs3Error> {
        let mut buf = BytesMut::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(Hfs3Error::Http)?;
            buf.extend_from_slice(&chunk);
        }
        let total = buf.len() as u64;
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
        _file_size: u64,
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
            }

            // Drain all remaining in-flight parts
            while let Some(result) = in_flight.join_next().await {
                let (pnum, part, bytes) = result
                    .map_err(|e| Hfs3Error::S3(format!("part upload task panicked: {e}")))??;
                completed_parts.push((pnum, part));
                total_bytes += bytes;
                on_part_uploaded(bytes);
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

        let resp = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| Hfs3Error::S3(format!("get_object failed for {key}: {e}")))?;

        let mut body = resp.body.into_async_read();
        let mut file = tokio::fs::File::create(dest).await?;
        let bytes_written = tokio::io::copy(&mut body, &mut file).await?;
        file.flush().await?;

        tracing::info!(
            bucket,
            key,
            dest = %dest.display(),
            bytes = bytes_written,
            "downloaded to file"
        );

        Ok(bytes_written)
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
    /// Strips the prefix from keys to form local paths.
    /// Returns (files_downloaded, total_bytes).
    pub async fn download_all(
        &self,
        bucket: &str,
        prefix: &str,
        dest_dir: &Path,
    ) -> Result<(usize, u64), Hfs3Error> {
        let objects = self.list_objects(bucket, prefix).await?;
        let mut files_downloaded: usize = 0;
        let mut total_bytes: u64 = 0;

        // Normalize prefix for stripping (ensure trailing slash)
        let strip_prefix = if prefix.ends_with('/') {
            prefix.to_string()
        } else {
            format!("{prefix}/")
        };

        for (key, _size) in &objects {
            // Strip the prefix to get relative path
            let relative = key.strip_prefix(&strip_prefix).unwrap_or(key);

            if relative.is_empty() {
                continue;
            }

            let dest_path = dest_dir.join(relative);
            let bytes = self.download_to_file(bucket, key, &dest_path).await?;
            total_bytes += bytes;
            files_downloaded += 1;
        }

        tracing::info!(
            bucket,
            prefix,
            files = files_downloaded,
            bytes = total_bytes,
            dest = %dest_dir.display(),
            "download_all complete"
        );

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
}
