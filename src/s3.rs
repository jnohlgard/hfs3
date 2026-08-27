//! S3 operations for hfs3: multipart upload, download, and listing.
//!
//! Uses aws-sdk-s3 for all S3 interactions. Supports streaming multipart
//! uploads (zero-copy from HF download stream) and adaptive chunk sizing.

use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client as S3Client;
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::error::Hfs3Error;

// Re-export chunk functions so consumers can use them via s3 module.
pub use crate::concurrency::{
    chunk_size_for_file, chunk_size_for_transfer, plan_transfer, plan_transfer_with_memory,
    PUT_OBJECT_THRESHOLD,
};

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

/// Suffix for the small manifest object pinning an in-flight multipart
/// upload's layout, so an interrupted upload can be resumed on re-run.
pub const RESUME_MANIFEST_SUFFIX: &str = ".hfs3-resume.json";

/// Pinned layout for an in-flight multipart upload.
///
/// Stored at `<key>.hfs3-resume.json` so a re-run can validate surviving
/// parts even if memory-based chunk sizing would pick a different chunk
/// size this time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeManifest {
    pub file_size: u64,
    pub chunk_size: usize,
    pub upload_id: String,
}

/// State of a resumable multipart upload: which parts already landed in S3.
#[derive(Debug, Clone)]
pub struct ResumeState {
    pub upload_id: String,
    pub chunk_size: usize,
    pub file_size: u64,
    /// Surviving parts as (part_number, size, etag).
    pub completed: Vec<(i32, u64, String)>,
}

impl ResumeState {
    /// Number of parts the pinned chunk layout implies.
    pub fn expected_parts(&self) -> i32 {
        (self.file_size.div_ceil(self.chunk_size as u64)) as i32
    }

    /// Smallest part number not yet present (1-based), or 0 if all present.
    pub fn min_part_to_upload(&self) -> i32 {
        let done: BTreeSet<i32> = self.completed.iter().map(|(p, _, _)| *p).collect();
        (1..=self.expected_parts())
            .find(|p| !done.contains(p))
            .unwrap_or(0)
    }

    /// Total bytes already in S3 across surviving parts.
    pub fn skipped_bytes(&self) -> u64 {
        self.completed.iter().map(|(_, s, _)| *s).sum()
    }

    /// True when every expected part is present and only completion is left.
    pub fn is_complete(&self) -> bool {
        self.min_part_to_upload() == 0
    }
}

/// Validate a surviving-part list against a pinned chunk layout.
///
/// Returns `Ok(first_missing_part)` (0 = all parts present) or `Err` when
/// the layout is invalid (bad chunk size, out-of-range/duplicate parts, or
/// a part with the wrong size) and the upload must not be reused.
pub fn validate_resume_parts(
    file_size: u64,
    chunk_size: usize,
    parts: &[(i32, u64)],
) -> Result<i32, String> {
    let chunk = chunk_size as u64;
    if chunk < 5 * 1024 * 1024 {
        return Err(format!("chunk size {chunk} below S3 5 MiB minimum"));
    }
    let total = file_size.div_ceil(chunk);
    if total > 10_000 {
        return Err(format!("{total} parts exceeds S3 10,000-part limit"));
    }
    let n = total as i32;
    let mut seen = BTreeSet::new();
    for (pnum, size) in parts {
        if !(*pnum >= 1 && *pnum <= n) {
            return Err(format!("part {pnum} outside 1..={n}"));
        }
        if !seen.insert(*pnum) {
            return Err(format!("duplicate part {pnum}"));
        }
        let expected = if *pnum == n {
            file_size - (n - 1) as u64 * chunk
        } else {
            chunk
        };
        if *size != expected {
            return Err(format!("part {pnum} is {size} bytes, expected {expected}"));
        }
    }
    for p in 1..=n {
        if !seen.contains(&p) {
            return Ok(p);
        }
    }
    Ok(0)
}

/// Failure classification for multipart uploads.
///
/// `Abort` means the local view is inconsistent (size mismatch, broken part
/// layout) and the upload must be cancelled. `Retryable` means the upload
/// left in S3 is intact: it is deliberately left in place so the next
/// `hfs3 mirror` run can resume from the surviving parts.
#[derive(Debug)]
enum MpFail {
    Abort(Hfs3Error),
    Retryable(Hfs3Error),
}

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
        self.upload_multipart_stream_with_progress(
            bucket,
            key,
            stream,
            file_size,
            &params,
            None,
            |_| {},
        )
        .await
    }

    /// Upload a byte stream with a per-part progress callback and tunable params.
    ///
    /// `on_part_uploaded` is called with the byte count after each S3 part
    /// (or put_object for small files) completes successfully. When `resume`
    /// is set, the surviving parts are reused and the upload completes
    /// against the existing multipart upload; the returned byte count is
    /// only the bytes moved by this call.
    #[allow(clippy::too_many_arguments)]
    pub async fn upload_multipart_stream_with_progress<F>(
        &self,
        bucket: &str,
        key: &str,
        stream: impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
        file_size: u64,
        params: &UploadParams,
        resume: Option<&ResumeState>,
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
            self.upload_multipart(
                bucket,
                key,
                stream,
                file_size,
                params,
                resume,
                on_part_uploaded,
            )
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
    /// Parts are buffered from the stream and uploaded via a JoinSet, bounded by
    /// `params.max_parts_in_flight`. A manifest is written at
    /// `<key>.hfs3-resume.json` pinning the chunk layout; on transport errors
    /// the upload is left in place so a re-run can resume (see `resolve_resume`),
    /// while integrity errors abort the upload and remove the manifest.
    ///
    /// When `resume` is set the upload continues an existing multipart upload:
    /// already-present parts are skipped and `stream` is expected to start at
    /// the first missing part (callers use an HTTP Range request for that).
    #[allow(clippy::too_many_arguments)]
    async fn upload_multipart<F>(
        &self,
        bucket: &str,
        key: &str,
        mut stream: impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
        file_size: u64,
        params: &UploadParams,
        resume: Option<&ResumeState>,
        on_part_uploaded: F,
    ) -> Result<u64, Hfs3Error>
    where
        F: Fn(u64),
    {
        let chunk_size = resume.map_or(params.chunk_size, |r| r.chunk_size);
        let max_in_flight = params.max_parts_in_flight;

        // Create or reuse the multipart upload
        let upload_id = match resume {
            Some(r) => r.upload_id.clone(),
            None => {
                let create_resp = self
                    .client
                    .create_multipart_upload()
                    .bucket(bucket)
                    .key(key)
                    .send()
                    .await
                    .map_err(|e| {
                        Hfs3Error::S3(format!("create_multipart_upload failed for {key}: {e}"))
                    })?;

                let upload_id = create_resp
                    .upload_id()
                    .ok_or_else(|| Hfs3Error::S3("no upload_id returned".into()))?
                    .to_string();

                self.put_resume_manifest(
                    bucket,
                    key,
                    &ResumeManifest {
                        file_size,
                        chunk_size,
                        upload_id: upload_id.clone(),
                    },
                )
                .await;

                upload_id
            }
        };

        let skip: BTreeSet<i32> = resume
            .map(|r| r.completed.iter().map(|(p, _, _)| *p).collect())
            .unwrap_or_default();
        let mut completed_parts: Vec<(i32, CompletedPart)> = resume
            .map(|r| {
                r.completed
                    .iter()
                    .map(|(p, _, etag)| {
                        (
                            *p,
                            CompletedPart::builder()
                                .e_tag(etag.clone())
                                .part_number(*p)
                                .build(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut part_number = resume.map_or(1, |r| r.min_part_to_upload().max(1));
        let mut total_bytes: u64 = resume.map_or(0, |r| r.skipped_bytes());
        let mut buf = BytesMut::with_capacity(chunk_size);
        let mut in_flight: JoinSet<Result<(i32, CompletedPart, u64), MpFail>> = JoinSet::new();

        let result: Result<(), MpFail> = async {
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| MpFail::Retryable(Hfs3Error::Http(e)))?;
                buf.extend_from_slice(&chunk);

                // Eagerly collect any completed uploads (non-blocking).
                // This keeps progress reporting responsive and frees memory
                // from finished parts without waiting for backpressure.
                while let Some(join_result) = in_flight.try_join_next() {
                    let (pnum, part, bytes) = join_result.map_err(|e| {
                        MpFail::Retryable(Hfs3Error::S3(format!("part upload task panicked: {e}")))
                    })??;
                    completed_parts.push((pnum, part));
                    total_bytes += bytes;
                    on_part_uploaded(bytes);
                }

                while buf.len() >= chunk_size {
                    // Part already landed in a previous run: bytes still pass
                    // through the stream but no upload is needed.
                    if skip.contains(&part_number) {
                        part_number += 1;
                        continue;
                    }

                    // If at capacity, wait for one in-flight part to complete
                    while in_flight.len() >= max_in_flight {
                        if let Some(join_result) = in_flight.join_next().await {
                            let (pnum, part, bytes) = join_result.map_err(|e| {
                                MpFail::Retryable(Hfs3Error::S3(format!(
                                    "part upload task panicked: {e}"
                                )))
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
                                MpFail::Retryable(Hfs3Error::S3(format!(
                                    "upload_part {pn} failed for {k}: {e}"
                                )))
                            })?;

                        let etag = resp
                            .e_tag()
                            .ok_or_else(|| {
                                MpFail::Retryable(Hfs3Error::S3(format!("no ETag for part {pn}")))
                            })?
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
                if !skip.contains(&part_number) {
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
                                MpFail::Retryable(Hfs3Error::S3(format!(
                                    "upload_part {pn} failed for {k}: {e}"
                                )))
                            })?;

                        let etag = resp
                            .e_tag()
                            .ok_or_else(|| {
                                MpFail::Retryable(Hfs3Error::S3(format!("no ETag for part {pn}")))
                            })?
                            .to_string();

                        let completed =
                            CompletedPart::builder().e_tag(etag).part_number(pn).build();

                        Ok((pn, completed, part_len))
                    });
                }
                part_number += 1;
            }

            // Drain all remaining in-flight parts
            while let Some(join_result) = in_flight.join_next().await {
                let (pnum, part, bytes) = join_result.map_err(|e| {
                    MpFail::Retryable(Hfs3Error::S3(format!("part upload task panicked: {e}")))
                })??;
                completed_parts.push((pnum, part));
                total_bytes += bytes;
                on_part_uploaded(bytes);
            }

            // Reject a stream whose actual size differs from the listed size
            // so a truncated download never lands in S3.
            if total_bytes != file_size {
                return Err(MpFail::Abort(Hfs3Error::S3(format!(
                    "size mismatch for {key}: listed {file_size} bytes, uploaded {total_bytes}"
                ))));
            }

            Ok(())
        }
        .await;

        // On failure, clean up in-flight tasks, then handle per failure class.
        if let Err(fail) = result {
            in_flight.abort_all();
            // Drain to ensure all tasks are cleaned up
            while in_flight.join_next().await.is_some() {}

            match fail {
                MpFail::Abort(e) => {
                    tracing::warn!(
                        bucket,
                        key,
                        upload_id = %upload_id,
                        "aborting multipart upload due to invalid local state"
                    );
                    let _ = self
                        .client
                        .abort_multipart_upload()
                        .bucket(bucket)
                        .key(key)
                        .upload_id(&upload_id)
                        .send()
                        .await;
                    self.delete_resume_manifest(bucket, key).await;
                    return Err(e);
                }
                MpFail::Retryable(e) => {
                    tracing::warn!(
                        bucket,
                        key,
                        upload_id = %upload_id,
                        "multipart upload left in place; rerun hfs3 mirror to resume"
                    );
                    return Err(e);
                }
            }
        }

        // Sort by part number (parts may complete out of order) and validate
        // contiguity against the expected part count.
        completed_parts.sort_by_key(|(pnum, _)| *pnum);
        let expected_count = file_size.div_ceil(chunk_size as u64) as usize;
        let contiguous = completed_parts
            .iter()
            .enumerate()
            .all(|(i, (pnum, _))| *pnum == i as i32 + 1);
        if completed_parts.len() != expected_count || !contiguous {
            let _ = self
                .client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .send()
                .await;
            self.delete_resume_manifest(bucket, key).await;
            return Err(Hfs3Error::S3(format!(
            "part layout mismatch for {key}: expected {expected_count} contiguous parts, got {}",
            completed_parts.len()
        )));
        }

        let parts: Vec<CompletedPart> = completed_parts.into_iter().map(|(_, part)| part).collect();

        // Complete the multipart upload. If this fails the parts are still in
        // S3, so leave the manifest in place for the next run to re-complete.
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

        self.delete_resume_manifest(bucket, key).await;

        let moved_bytes = total_bytes - resume.map_or(0, |r| r.skipped_bytes());
        tracing::info!(
            bucket,
            key,
            moved_bytes,
            parts = expected_count,
            max_in_flight,
            chunk_size_mb = chunk_size / (1024 * 1024),
            resumed = resume.is_some(),
            "multipart upload complete"
        );

        Ok(moved_bytes)
    }

    /// List in-progress multipart uploads under a prefix (key, upload_id).
    pub async fn list_incomplete_uploads(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<(String, String)>, Hfs3Error> {
        let mut uploads: Vec<(String, String)> = Vec::new();
        let mut key_marker: Option<String> = None;
        let mut upload_id_marker: Option<String> = None;

        loop {
            let mut req = self
                .client
                .list_multipart_uploads()
                .bucket(bucket)
                .prefix(prefix);
            if let Some(m) = key_marker.take() {
                req = req.key_marker(m);
            }
            if let Some(m) = upload_id_marker.take() {
                req = req.upload_id_marker(m);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| Hfs3Error::S3(format!("list_multipart_uploads failed: {e}")))?;

            for up in resp.uploads() {
                if let (Some(k), Some(u)) = (up.key(), up.upload_id()) {
                    uploads.push((k.to_string(), u.to_string()));
                }
            }

            if resp.is_truncated() == Some(true) {
                key_marker = resp.next_key_marker().map(String::from);
                upload_id_marker = resp.next_upload_id_marker().map(String::from);
            } else {
                break;
            }
        }

        tracing::info!(
            bucket,
            prefix,
            count = uploads.len(),
            "listed in-progress uploads"
        );
        Ok(uploads)
    }

    /// Fetch the resume manifest for a key, if one exists.
    async fn get_resume_manifest(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<ResumeManifest>, Hfs3Error> {
        let manifest_key = format!("{key}{RESUME_MANIFEST_SUFFIX}");
        let resp = match self
            .client
            .get_object()
            .bucket(bucket)
            .key(&manifest_key)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                let not_found = e
                    .as_service_error()
                    .and_then(|s| s.code())
                    .is_some_and(|c| c == "NoSuchKey" || c == "NotFound");
                return if not_found {
                    Ok(None)
                } else {
                    Err(Hfs3Error::S3(format!("get resume manifest for {key}: {e}")))
                };
            }
        };

        let collected = resp
            .body
            .collect()
            .await
            .map_err(|e| Hfs3Error::S3(format!("read resume manifest for {key}: {e}")))?;
        let manifest: ResumeManifest = serde_json::from_slice(&collected.into_bytes())
            .map_err(|e| Hfs3Error::S3(format!("parse resume manifest for {key}: {e}")))?;
        Ok(Some(manifest))
    }

    /// Write the resume manifest for a key. Best-effort: on failure resume is
    /// simply disabled for this upload, the transfer itself proceeds.
    async fn put_resume_manifest(&self, bucket: &str, key: &str, manifest: &ResumeManifest) {
        let manifest_key = format!("{key}{RESUME_MANIFEST_SUFFIX}");
        let body = serde_json::to_vec(manifest).expect("ResumeManifest serializes");
        if let Err(e) = self
            .client
            .put_object()
            .bucket(bucket)
            .key(&manifest_key)
            .body(ByteStream::from(body))
            .send()
            .await
        {
            tracing::warn!(
                "failed to write resume manifest for {key}: {e} (resume disabled for this upload)"
            );
        }
    }

    /// Delete the resume manifest for a key. Best-effort.
    async fn delete_resume_manifest(&self, bucket: &str, key: &str) {
        let manifest_key = format!("{key}{RESUME_MANIFEST_SUFFIX}");
        if let Err(e) = self
            .client
            .delete_object()
            .bucket(bucket)
            .key(&manifest_key)
            .send()
            .await
        {
            tracing::debug!("failed to delete resume manifest for {key}: {e}");
        }
    }

    /// Abort a multipart upload and remove its manifest (both best-effort).
    pub async fn abandon_upload(&self, bucket: &str, key: &str, upload_id: Option<&str>) {
        if let Some(uid) = upload_id {
            if let Err(e) = self
                .client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(uid)
                .send()
                .await
            {
                tracing::debug!("failed to abort upload {uid} for {key}: {e}");
            }
        }
        self.delete_resume_manifest(bucket, key).await;
    }

    /// Decide whether an in-progress upload for `key` can be resumed.
    ///
    /// Cross-checks the manifest (pinned chunk layout) against ListParts.
    /// Returns `None` (having cleaned up any unusable state) when there is
    /// no manifest, the upload is gone, or the surviving parts do not match
    /// the pinned layout; `Some(state)` when resume is possible.
    pub async fn resolve_resume(
        &self,
        bucket: &str,
        key: &str,
        file_size: u64,
        candidate_upload_id: Option<&str>,
    ) -> Result<Option<ResumeState>, Hfs3Error> {
        let manifest = match self.get_resume_manifest(bucket, key).await? {
            Some(m) => m,
            None => {
                // No manifest: the candidate upload cannot be validated.
                self.abandon_upload(bucket, key, candidate_upload_id).await;
                return Ok(None);
            }
        };

        let matches = candidate_upload_id.is_some_and(|c| c == manifest.upload_id);
        let upload_id = if matches {
            manifest.upload_id.clone()
        } else {
            // Candidate differs from the manifest (or none): trust the
            // manifest and abandon the orphaned candidate.
            self.abandon_upload(bucket, key, candidate_upload_id).await;
            manifest.upload_id.clone()
        };

        let mut parts: Vec<(i32, u64, String)> = Vec::new();
        let mut part_number_marker: Option<String> = None;
        loop {
            let mut req = self
                .client
                .list_parts()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id);
            if let Some(m) = part_number_marker.take() {
                req = req.part_number_marker(m);
            }
            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    let gone = e
                        .as_service_error()
                        .and_then(|s| s.code())
                        .is_some_and(|c| c == "NoSuchUpload" || c == "NotFound");
                    if gone {
                        self.delete_resume_manifest(bucket, key).await;
                        return Ok(None);
                    }
                    return Err(Hfs3Error::S3(format!("list_parts failed for {key}: {e}")));
                }
            };
            for p in resp.parts() {
                if let (Some(pn), Some(sz), Some(etag)) = (p.part_number(), p.size(), p.e_tag()) {
                    parts.push((pn, sz as u64, etag.to_string()));
                }
            }
            if resp.is_truncated() == Some(true) {
                part_number_marker = resp.next_part_number_marker().map(String::from);
            } else {
                break;
            }
        }

        let sizes: Vec<(i32, u64)> = parts.iter().map(|(p, s, _)| (*p, *s)).collect();
        match validate_resume_parts(file_size, manifest.chunk_size, &sizes) {
            Ok(_) => {
                parts.sort_by_key(|(p, _, _)| *p);
                Ok(Some(ResumeState {
                    upload_id,
                    chunk_size: manifest.chunk_size,
                    file_size,
                    completed: parts,
                }))
            }
            Err(reason) => {
                tracing::warn!(key, %reason, "resume layout invalid, starting fresh");
                self.abandon_upload(bucket, key, Some(&upload_id)).await;
                Ok(None)
            }
        }
    }

    /// Complete a multipart upload from already-recorded parts (all parts
    /// present; no download needed). Returns `file_size`.
    pub async fn complete_from_resume(
        &self,
        bucket: &str,
        key: &str,
        resume: &ResumeState,
    ) -> Result<u64, Hfs3Error> {
        let parts: Vec<CompletedPart> = resume
            .completed
            .iter()
            .map(|(p, _, etag)| {
                CompletedPart::builder()
                    .e_tag(etag.clone())
                    .part_number(*p)
                    .build()
            })
            .collect();
        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();

        self.client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(&resume.upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(|e| {
                Hfs3Error::S3(format!("complete_multipart_upload failed for {key}: {e}"))
            })?;

        self.delete_resume_manifest(bucket, key).await;
        Ok(resume.file_size)
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
                    if key.ends_with(RESUME_MANIFEST_SUFFIX) {
                        continue;
                    }
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

    #[test]
    fn validate_resume_all_parts_present() {
        // 26 MB file, 8 MB chunks -> 4 parts (8+8+8+2)
        let parts = [
            (1, 8 * MB as u64),
            (2, 8 * MB as u64),
            (3, 8 * MB as u64),
            (4, 2 * MB as u64),
        ];
        assert_eq!(validate_resume_parts(26 * MB as u64, 8 * MB, &parts), Ok(0));
    }

    #[test]
    fn validate_resume_gap_reports_first_missing() {
        let parts = [(1, 8 * MB as u64), (3, 8 * MB as u64), (4, 2 * MB as u64)];
        assert_eq!(validate_resume_parts(26 * MB as u64, 8 * MB, &parts), Ok(2));
    }

    #[test]
    fn validate_resume_first_part_missing() {
        let parts = [(2, 8 * MB as u64), (3, 8 * MB as u64), (4, 2 * MB as u64)];
        assert_eq!(validate_resume_parts(26 * MB as u64, 8 * MB, &parts), Ok(1));
    }

    #[test]
    fn validate_resume_wrong_part_size() {
        let parts = [
            (1, 7 * MB as u64),
            (2, 8 * MB as u64),
            (3, 8 * MB as u64),
            (4, 2 * MB as u64),
        ];
        let err = validate_resume_parts(26 * MB as u64, 8 * MB, &parts).unwrap_err();
        assert!(err.contains("part 1 is"));
    }

    #[test]
    fn validate_resume_out_of_range_part() {
        let parts = [(1, 8 * MB as u64), (5, 8 * MB as u64)];
        let err = validate_resume_parts(26 * MB as u64, 8 * MB, &parts).unwrap_err();
        assert!(err.contains("outside 1..=4"));
    }

    #[test]
    fn validate_resume_duplicate_part() {
        let parts = [
            (2, 8 * MB as u64),
            (2, 8 * MB as u64),
            (3, 8 * MB as u64),
            (4, 2 * MB as u64),
        ];
        let err = validate_resume_parts(26 * MB as u64, 8 * MB, &parts).unwrap_err();
        assert!(err.contains("duplicate part 2"));
    }

    #[test]
    fn validate_resume_single_part_file() {
        // File exactly one chunk: a single final part.
        let parts = [(1, 8 * MB as u64)];
        assert_eq!(validate_resume_parts(8 * MB as u64, 8 * MB, &parts), Ok(0));
        assert_eq!(validate_resume_parts(8 * MB as u64, 8 * MB, &[]), Ok(1));
    }

    #[test]
    fn validate_resume_chunk_below_minimum() {
        let parts: Vec<(i32, u64)> = Vec::new();
        let err = validate_resume_parts(10 * MB as u64, 4 * MB, &parts).unwrap_err();
        assert!(err.contains("5 MiB minimum"));
    }

    #[test]
    fn validate_resume_too_many_parts() {
        // 64 MiB chunks, 10,001 parts would be needed
        let parts: Vec<(i32, u64)> = Vec::new();
        let err = validate_resume_parts(64 * MB as u64 * 10_001, 64 * MB, &parts).unwrap_err();
        assert!(err.contains("10,000-part limit"));
    }

    #[test]
    fn resume_state_min_part_and_skipped_bytes() {
        let state = ResumeState {
            upload_id: "uid".into(),
            chunk_size: 8 * MB,
            file_size: 26 * MB as u64,
            completed: vec![
                (1, 8 * MB as u64, "e1".into()),
                (3, 8 * MB as u64, "e3".into()),
            ],
        };
        assert_eq!(state.expected_parts(), 4);
        assert_eq!(state.min_part_to_upload(), 2);
        assert_eq!(state.skipped_bytes(), 16 * MB as u64);
        assert!(!state.is_complete());
    }

    #[test]
    fn resume_state_complete() {
        let state = ResumeState {
            upload_id: "uid".into(),
            chunk_size: 8 * MB,
            file_size: 26 * MB as u64,
            completed: vec![
                (1, 8 * MB as u64, "e1".into()),
                (2, 8 * MB as u64, "e2".into()),
                (3, 8 * MB as u64, "e3".into()),
                (4, 2 * MB as u64, "e4".into()),
            ],
        };
        assert_eq!(state.min_part_to_upload(), 0);
        assert!(state.is_complete());
        assert_eq!(state.skipped_bytes(), 26 * MB as u64);
    }
}
