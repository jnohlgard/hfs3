//! Transfer statistics for performance diagnostics.
//!
//! Provides lock-free atomic counters for tracking download/upload progress,
//! per-file state, process memory sampling, and a background progress reporter.
//!
//! Hot-path operations (byte counting, part completion) use atomics.
//! Cold-path operations (memory sampling, worker file names) use a Mutex.

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::Stream;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::Serialize;

use crate::concurrency::{chunk_size_for_file, chunk_size_for_transfer};

// --- File state constants ---
const FILE_PENDING: usize = 0;
const FILE_ACTIVE: usize = 1;
const FILE_DONE: usize = 2;
const FILE_FAILED: usize = 3;

/// Per-file progress tracked with atomic counters.
pub struct FileProgress {
    pub bytes_downloaded: AtomicU64,
    pub bytes_uploaded: AtomicU64,
    pub chunks_uploaded: AtomicUsize,
    pub state: AtomicUsize,
}

impl FileProgress {
    fn new() -> Self {
        Self {
            bytes_downloaded: AtomicU64::new(0),
            bytes_uploaded: AtomicU64::new(0),
            chunks_uploaded: AtomicUsize::new(0),
            state: AtomicUsize::new(FILE_PENDING),
        }
    }
}

/// Shared transfer statistics. All hot-path counters are lock-free atomics.
pub struct TransferStats {
    pub start: Instant,

    // Immutable after creation
    pub total_files: usize,
    pub total_bytes: u64,
    pub total_chunks: usize,
    pub max_concurrent: usize,

    // Global atomic counters
    pub bytes_downloaded: AtomicU64,
    pub bytes_uploaded: AtomicU64,
    pub files_completed: AtomicUsize,
    pub files_failed: AtomicUsize,
    pub chunks_uploaded: AtomicUsize,
    pub active_workers: AtomicUsize,
    pub peak_workers: AtomicUsize,

    // Per-file state (indexed by file position in the original list)
    pub file_progress: Vec<FileProgress>,
    pub file_names: Vec<String>,
    pub file_sizes: Vec<u64>,

    // Process RSS samples (cold path — sampled every few seconds)
    rss_samples: Mutex<Vec<u64>>,
}

impl TransferStats {
    /// Create stats for a set of files.
    pub fn new(files: &[(String, u64)], max_concurrent: usize, available_memory: u64) -> Self {
        let total_bytes: u64 = files.iter().map(|(_, s)| *s).sum();
        let total_chunks = estimate_total_chunks(files, available_memory);

        let file_progress: Vec<FileProgress> =
            (0..files.len()).map(|_| FileProgress::new()).collect();
        let file_names: Vec<String> = files.iter().map(|(n, _)| n.clone()).collect();
        let file_sizes: Vec<u64> = files.iter().map(|(_, s)| *s).collect();

        Self {
            start: Instant::now(),
            total_files: files.len(),
            total_bytes,
            total_chunks,
            max_concurrent,
            bytes_downloaded: AtomicU64::new(0),
            bytes_uploaded: AtomicU64::new(0),
            files_completed: AtomicUsize::new(0),
            files_failed: AtomicUsize::new(0),
            chunks_uploaded: AtomicUsize::new(0),
            active_workers: AtomicUsize::new(0),
            peak_workers: AtomicUsize::new(0),
            file_progress,
            file_names,
            file_sizes,
            rss_samples: Mutex::new(Vec::new()),
        }
    }

    /// Begin tracking a file. Returns a guard that ensures cleanup on drop.
    ///
    /// The guard MUST be explicitly completed via `guard.complete()`.
    /// If dropped without completing (panic, early return, error),
    /// the file is marked failed and active_workers is decremented.
    pub fn begin_file(self: &Arc<Self>, file_idx: usize) -> WorkerGuard {
        self.file_progress[file_idx]
            .state
            .store(FILE_ACTIVE, Ordering::Relaxed);
        let active = self.active_workers.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak_workers.fetch_max(active, Ordering::Relaxed);
        WorkerGuard {
            stats: Arc::clone(self),
            file_idx,
            finished: false,
        }
    }

    fn mark_completed(&self, file_idx: usize) {
        self.file_progress[file_idx]
            .state
            .store(FILE_DONE, Ordering::Relaxed);
        self.files_completed.fetch_add(1, Ordering::Relaxed);
        self.active_workers.fetch_sub(1, Ordering::Relaxed);
    }

    fn mark_failed(&self, file_idx: usize) {
        self.file_progress[file_idx]
            .state
            .store(FILE_FAILED, Ordering::Relaxed);
        self.files_failed.fetch_add(1, Ordering::Relaxed);
        self.active_workers.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record bytes downloaded (called from CountingStream callback).
    pub fn add_downloaded(&self, file_idx: usize, bytes: u64) {
        self.bytes_downloaded.fetch_add(bytes, Ordering::Relaxed);
        self.file_progress[file_idx]
            .bytes_downloaded
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record an S3 part upload completing.
    pub fn part_uploaded(&self, file_idx: usize, bytes: u64) {
        self.bytes_uploaded.fetch_add(bytes, Ordering::Relaxed);
        self.file_progress[file_idx]
            .bytes_uploaded
            .fetch_add(bytes, Ordering::Relaxed);
        self.chunks_uploaded.fetch_add(1, Ordering::Relaxed);
        self.file_progress[file_idx]
            .chunks_uploaded
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Sample current process RSS from /proc/self/status.
    pub fn sample_memory(&self) {
        if let Some(rss) = process_rss_bytes() {
            if let Ok(mut samples) = self.rss_samples.lock() {
                samples.push(rss);
            }
        }
    }

    /// Compute (mean_rss, peak_rss) from samples.
    fn memory_stats(&self) -> (u64, u64) {
        let samples = self.rss_samples.lock().unwrap_or_else(|e| e.into_inner());
        if samples.is_empty() {
            return (0, 0);
        }
        let sum: u64 = samples.iter().sum();
        let mean = sum / samples.len() as u64;
        let peak = *samples.iter().max().unwrap_or(&0);
        (mean, peak)
    }

    /// Take a point-in-time snapshot for reporting.
    pub fn snapshot(&self) -> StatsSnapshot {
        let elapsed = self.start.elapsed();
        let dl = self.bytes_downloaded.load(Ordering::Relaxed);
        let ul = self.bytes_uploaded.load(Ordering::Relaxed);
        let elapsed_secs = elapsed.as_secs_f64().max(0.001);

        let (mean_rss, peak_rss) = self.memory_stats();

        StatsSnapshot {
            elapsed,
            total_files: self.total_files,
            files_completed: self.files_completed.load(Ordering::Relaxed),
            files_failed: self.files_failed.load(Ordering::Relaxed),
            total_bytes: self.total_bytes,
            bytes_downloaded: dl,
            bytes_uploaded: ul,
            download_mbps: (dl as f64 / elapsed_secs) / (1024.0 * 1024.0),
            upload_mbps: (ul as f64 / elapsed_secs) / (1024.0 * 1024.0),
            total_chunks: self.total_chunks,
            chunks_uploaded: self.chunks_uploaded.load(Ordering::Relaxed),
            active_workers: self.active_workers.load(Ordering::Relaxed),
            peak_workers: self.peak_workers.load(Ordering::Relaxed),
            mean_rss_bytes: mean_rss,
            peak_rss_bytes: peak_rss,
        }
    }

    /// Generate the final report for JSON output.
    pub fn report(&self) -> TransferStatsReport {
        let snap = self.snapshot();
        TransferStatsReport {
            download_mbps: (snap.download_mbps * 100.0).round() / 100.0,
            upload_mbps: (snap.upload_mbps * 100.0).round() / 100.0,
            mean_rss_mb: (snap.mean_rss_bytes as f64 / (1024.0 * 1024.0) * 10.0).round() / 10.0,
            peak_rss_mb: (snap.peak_rss_bytes as f64 / (1024.0 * 1024.0) * 10.0).round() / 10.0,
            total_chunks: snap.total_chunks,
            chunks_uploaded: snap.chunks_uploaded,
            total_bytes_expected: snap.total_bytes,
            peak_concurrent_workers: snap.peak_workers,
        }
    }
}

/// RAII guard for worker lifecycle. Marks file as failed on drop
/// unless explicitly completed. Prevents leaked active_workers count
/// on panics, early returns, or task cancellation.
pub struct WorkerGuard {
    stats: Arc<TransferStats>,
    file_idx: usize,
    finished: bool,
}

impl WorkerGuard {
    /// Mark the file as successfully completed. Consumes the guard.
    pub fn complete(mut self) {
        self.finished = true;
        self.stats.mark_completed(self.file_idx);
    }
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.stats.mark_failed(self.file_idx);
        }
    }
}

// --- Snapshot & Report types ---

/// Point-in-time snapshot of transfer statistics.
#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    pub elapsed: Duration,
    pub total_files: usize,
    pub files_completed: usize,
    pub files_failed: usize,
    pub total_bytes: u64,
    pub bytes_downloaded: u64,
    pub bytes_uploaded: u64,
    pub download_mbps: f64,
    pub upload_mbps: f64,
    pub total_chunks: usize,
    pub chunks_uploaded: usize,
    pub active_workers: usize,
    pub peak_workers: usize,
    pub mean_rss_bytes: u64,
    pub peak_rss_bytes: u64,
}

/// Final statistics report, embedded in the JSON output.
#[derive(Debug, Clone, Serialize)]
pub struct TransferStatsReport {
    pub download_mbps: f64,
    pub upload_mbps: f64,
    pub mean_rss_mb: f64,
    pub peak_rss_mb: f64,
    pub total_chunks: usize,
    pub chunks_uploaded: usize,
    pub total_bytes_expected: u64,
    pub peak_concurrent_workers: usize,
}

// --- Chunk estimation ---

const PUT_THRESHOLD: u64 = 8 * 1024 * 1024;

/// Estimate total S3 parts across all files.
///
/// Files below the put_object threshold count as 1 chunk.
/// Larger files use memory-aware chunk sizing when available_memory > 0,
/// otherwise falls back to file-size-only tiers.
pub fn estimate_total_chunks(files: &[(String, u64)], available_memory: u64) -> usize {
    files
        .iter()
        .map(|(_, size)| {
            if *size < PUT_THRESHOLD {
                1
            } else {
                let cs = if available_memory > 0 {
                    chunk_size_for_transfer(*size, available_memory) as u64
                } else {
                    chunk_size_for_file(*size) as u64
                };
                (*size).div_ceil(cs) as usize
            }
        })
        .sum()
}

// --- Process memory ---

/// Read VmRSS (resident set size) from /proc/self/status.
/// Returns bytes, or None if unavailable.
pub fn process_rss_bytes() -> Option<u64> {
    let contents = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in contents.lines() {
        if line.starts_with("VmRSS:") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let kb: u64 = parts[1].parse().ok()?;
                return Some(kb * 1024);
            }
        }
    }
    None
}

// --- CountingStream ---

/// A stream wrapper that calls `on_bytes` for each successful chunk.
///
/// Wraps any `Stream<Item = Result<Bytes, E>>` to count bytes flowing
/// through without modifying the data. Zero overhead beyond the callback.
pub struct CountingStream<S, F> {
    inner: S,
    on_bytes: F,
}

impl<S, F> CountingStream<S, F> {
    pub fn new(inner: S, on_bytes: F) -> Self {
        Self { inner, on_bytes }
    }
}

impl<S, E, F> Stream for CountingStream<S, F>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    F: Fn(usize) + Unpin,
{
    type Item = Result<Bytes, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                (this.on_bytes)(bytes.len());
                Poll::Ready(Some(Ok(bytes)))
            }
            other => other,
        }
    }
}

// --- Formatting helpers ---

pub(crate) fn fmt_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * 1024;
    const GB: u64 = 1024 * 1024 * 1024;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

// --- Progress reporter ---

/// Spawn a background task that renders transfer progress using indicatif.
///
/// Returns a `MultiProgress` handle (use `mp.println()` for messages during
/// transfer) and a `JoinHandle` that self-terminates when all files are done.
///
/// The display has two bars:
/// - Main bar: total upload progress with speeds, file count, RSS
/// - Status bar: active file detail (name, upload progress, parts)
///
/// File completions and failures are printed as "✓" / "✗" lines above the bars.
pub fn spawn_progress_reporter(
    stats: Arc<TransferStats>,
    interval: Duration,
) -> (MultiProgress, tokio::task::JoinHandle<()>) {
    let mp = MultiProgress::new();

    // Main progress bar — tracks total upload bytes
    let main_bar = mp.add(ProgressBar::new(stats.total_bytes));
    main_bar.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({percent}%) {msg}",
        )
        .unwrap()
        .progress_chars("━╸─"),
    );
    main_bar.enable_steady_tick(Duration::from_millis(120));

    // Status bar — shows active file detail below the main bar
    let status_bar = mp.add(ProgressBar::new_spinner());
    status_bar.set_style(ProgressStyle::with_template("  {msg}").unwrap());

    let mp_for_task = mp.clone();

    let handle = tokio::spawn(async move {
        let mut last_states = vec![FILE_PENDING; stats.total_files];

        loop {
            tokio::time::sleep(interval).await;

            stats.sample_memory();
            let snap = stats.snapshot();

            // Update main bar
            main_bar.set_position(snap.bytes_uploaded);

            let rss = if snap.peak_rss_bytes > 0 {
                format!("  RSS {}", fmt_bytes(snap.peak_rss_bytes))
            } else {
                String::new()
            };

            main_bar.set_message(format!(
                "↓ {:.1} MB/s  ↑ {:.1} MB/s  {}/{} files  {}w{}",
                snap.download_mbps,
                snap.upload_mbps,
                snap.files_completed,
                snap.total_files,
                snap.active_workers,
                rss,
            ));

            // Detect state transitions + build active file summaries
            let mut active_summaries = Vec::new();

            for (i, fp) in stats.file_progress.iter().enumerate() {
                let state = fp.state.load(Ordering::Relaxed);

                if state == FILE_DONE && last_states[i] != FILE_DONE {
                    mp_for_task
                        .println(format!(
                            "  ✓ {} ({})",
                            stats.file_names[i],
                            fmt_bytes(stats.file_sizes[i]),
                        ))
                        .ok();
                }

                if state == FILE_FAILED && last_states[i] != FILE_FAILED {
                    mp_for_task
                        .println(format!("  ✗ {} failed", stats.file_names[i]))
                        .ok();
                }

                if state == FILE_ACTIVE {
                    let ul = fp.bytes_uploaded.load(Ordering::Relaxed);
                    let fsize = stats.file_sizes[i];
                    let parts = fp.chunks_uploaded.load(Ordering::Relaxed);

                    let name = &stats.file_names[i];
                    let short = if name.len() > 25 {
                        format!("…{}", &name[name.len() - 24..])
                    } else {
                        name.clone()
                    };

                    active_summaries.push(format!(
                        "{}: ↑{}/{}  p:{}",
                        short,
                        fmt_bytes(ul),
                        fmt_bytes(fsize),
                        parts,
                    ));
                }

                last_states[i] = state;
            }

            if active_summaries.is_empty() {
                status_bar.set_message(String::new());
            } else {
                status_bar.set_message(active_summaries.join("  │  "));
            }

            // Self-terminate when all files are done
            let done = snap.files_completed + snap.files_failed;
            if done >= snap.total_files {
                main_bar.finish_with_message("done ✓");
                status_bar.finish_and_clear();
                break;
            }
        }
    });

    (mp, handle)
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use futures::StreamExt;

    #[test]
    fn test_estimate_total_chunks_small_files() {
        let files = vec![
            ("a.txt".to_string(), 1024u64),
            ("b.txt".to_string(), 4 * 1024 * 1024),
        ];
        // Both < 8 MB threshold -> 1 chunk each
        assert_eq!(estimate_total_chunks(&files, 0), 2);
    }

    #[test]
    fn test_estimate_total_chunks_mixed() {
        let files = vec![
            ("small.txt".to_string(), 1024u64),
            ("medium.bin".to_string(), 100 * 1024 * 1024),
            ("large.bin".to_string(), 2 * 1024 * 1024 * 1024),
        ];
        // small: 1 (put_object)
        // medium: 100 MB / 8 MB = ceil(12.5) = 13
        // large: 2 GB / 64 MB = 32
        assert_eq!(estimate_total_chunks(&files, 0), 1 + 13 + 32);
    }

    #[test]
    fn test_estimate_total_chunks_empty() {
        assert_eq!(estimate_total_chunks(&[], 0), 0);
    }

    #[test]
    fn test_estimate_chunks_exact_boundary() {
        // Exactly 8 MB -> multipart (8 MB / 8 MB = 1 part)
        let files = vec![("exact.bin".to_string(), 8 * 1024 * 1024)];
        assert_eq!(estimate_total_chunks(&files, 0), 1);
    }

    #[test]
    fn test_transfer_stats_new() {
        let files = vec![
            ("a.txt".to_string(), 1000u64),
            ("b.txt".to_string(), 2000u64),
        ];
        let stats = TransferStats::new(&files, 4, 0);
        assert_eq!(stats.total_files, 2);
        assert_eq!(stats.total_bytes, 3000);
        assert_eq!(stats.max_concurrent, 4);
        assert_eq!(stats.bytes_downloaded.load(Ordering::Relaxed), 0);
        assert_eq!(stats.bytes_uploaded.load(Ordering::Relaxed), 0);
        assert_eq!(stats.file_names, vec!["a.txt", "b.txt"]);
        assert_eq!(stats.file_sizes, vec![1000, 2000]);
    }

    #[test]
    fn test_worker_lifecycle() {
        let files = vec![("a.txt".to_string(), 1000u64)];
        let stats = Arc::new(TransferStats::new(&files, 4, 0));

        let guard = stats.begin_file(0);
        assert_eq!(stats.active_workers.load(Ordering::Relaxed), 1);
        assert_eq!(stats.peak_workers.load(Ordering::Relaxed), 1);
        assert_eq!(
            stats.file_progress[0].state.load(Ordering::Relaxed),
            FILE_ACTIVE
        );

        guard.complete();
        assert_eq!(stats.active_workers.load(Ordering::Relaxed), 0);
        assert_eq!(stats.files_completed.load(Ordering::Relaxed), 1);
        assert_eq!(
            stats.file_progress[0].state.load(Ordering::Relaxed),
            FILE_DONE
        );
    }

    #[test]
    fn test_worker_failure_on_drop() {
        let files = vec![("a.txt".to_string(), 1000u64)];
        let stats = Arc::new(TransferStats::new(&files, 4, 0));

        {
            let _guard = stats.begin_file(0);
            // guard dropped without complete() -> marks failed
        }
        assert_eq!(stats.active_workers.load(Ordering::Relaxed), 0);
        assert_eq!(stats.files_failed.load(Ordering::Relaxed), 1);
        assert_eq!(
            stats.file_progress[0].state.load(Ordering::Relaxed),
            FILE_FAILED
        );
    }

    #[test]
    fn test_byte_tracking() {
        let files = vec![("a.txt".to_string(), 1000u64)];
        let stats = TransferStats::new(&files, 4, 0);

        stats.add_downloaded(0, 500);
        stats.add_downloaded(0, 500);
        assert_eq!(stats.bytes_downloaded.load(Ordering::Relaxed), 1000);
        assert_eq!(
            stats.file_progress[0]
                .bytes_downloaded
                .load(Ordering::Relaxed),
            1000
        );

        stats.part_uploaded(0, 1000);
        assert_eq!(stats.bytes_uploaded.load(Ordering::Relaxed), 1000);
        assert_eq!(stats.chunks_uploaded.load(Ordering::Relaxed), 1);
        assert_eq!(
            stats.file_progress[0]
                .chunks_uploaded
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_peak_workers() {
        let files = vec![
            ("a.txt".to_string(), 1000u64),
            ("b.txt".to_string(), 2000u64),
            ("c.txt".to_string(), 3000u64),
        ];
        let stats = Arc::new(TransferStats::new(&files, 4, 0));

        let g0 = stats.begin_file(0);
        let g1 = stats.begin_file(1);
        let g2 = stats.begin_file(2);
        assert_eq!(stats.peak_workers.load(Ordering::Relaxed), 3);

        g0.complete();
        g1.complete();
        // Peak doesn't decrease
        assert_eq!(stats.peak_workers.load(Ordering::Relaxed), 3);
        assert_eq!(stats.active_workers.load(Ordering::Relaxed), 1);
        g2.complete();
    }

    #[test]
    fn test_snapshot_rates() {
        let files = vec![("a.txt".to_string(), 1_000_000u64)];
        let stats = TransferStats::new(&files, 4, 0);
        stats.bytes_downloaded.store(1_000_000, Ordering::Relaxed);
        stats.bytes_uploaded.store(500_000, Ordering::Relaxed);

        let snap = stats.snapshot();
        assert!(snap.download_mbps > 0.0);
        assert!(snap.upload_mbps > 0.0);
        // More downloaded than uploaded -> higher DL rate
        assert!(snap.download_mbps > snap.upload_mbps);
    }

    #[test]
    fn test_report_serializes() {
        let files = vec![("a.txt".to_string(), 1000u64)];
        let stats = TransferStats::new(&files, 4, 0);
        let report = stats.report();
        let json = serde_json::to_string(&report).expect("report should serialize");
        assert!(json.contains("download_mbps"));
        assert!(json.contains("upload_mbps"));
        assert!(json.contains("mean_rss_mb"));
        assert!(json.contains("peak_rss_mb"));
        assert!(json.contains("total_chunks"));
    }

    #[test]
    fn test_memory_stats_empty() {
        let files = vec![("a.txt".to_string(), 1000u64)];
        let stats = TransferStats::new(&files, 4, 0);
        let (mean, peak) = stats.memory_stats();
        assert_eq!(mean, 0);
        assert_eq!(peak, 0);
    }

    #[test]
    fn test_memory_stats_with_samples() {
        let files = vec![("a.txt".to_string(), 1000u64)];
        let stats = TransferStats::new(&files, 4, 0);

        {
            let mut s = stats.rss_samples.lock().unwrap();
            s.push(100);
            s.push(200);
            s.push(300);
        }

        let (mean, peak) = stats.memory_stats();
        assert_eq!(mean, 200);
        assert_eq!(peak, 300);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_process_rss_bytes() {
        let rss = process_rss_bytes();
        assert!(rss.is_some(), "/proc/self/status should be readable");
        let rss = rss.unwrap();
        assert!(rss > 0, "RSS should be positive");
        // Sanity: less than 100 GB
        assert!(rss < 100 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_fmt_bytes() {
        assert_eq!(fmt_bytes(0), "0 B");
        assert_eq!(fmt_bytes(500), "500 B");
        assert_eq!(fmt_bytes(1024), "1.0 KB");
        assert_eq!(fmt_bytes(1536), "1.5 KB");
        assert_eq!(fmt_bytes(1048576), "1.0 MB");
        assert_eq!(fmt_bytes(1073741824), "1.0 GB");
        assert_eq!(fmt_bytes(1610612736), "1.5 GB");
    }

    #[tokio::test]
    async fn test_counting_stream() {
        let counter = Arc::new(AtomicU64::new(0));
        let counter_clone = Arc::clone(&counter);

        let chunks: Vec<Result<Bytes, std::io::Error>> = vec![
            Ok(Bytes::from(vec![0u8; 100])),
            Ok(Bytes::from(vec![0u8; 200])),
            Ok(Bytes::from(vec![0u8; 300])),
        ];
        let inner = stream::iter(chunks);

        let mut counting = CountingStream::new(inner, move |n: usize| {
            counter_clone.fetch_add(n as u64, Ordering::Relaxed);
        });

        let mut total = 0u64;
        while let Some(Ok(bytes)) = counting.next().await {
            total += bytes.len() as u64;
        }

        assert_eq!(total, 600);
        assert_eq!(counter.load(Ordering::Relaxed), 600);
    }

    #[tokio::test]
    async fn test_counting_stream_propagates_errors() {
        let counter = Arc::new(AtomicU64::new(0));
        let counter_clone = Arc::clone(&counter);

        let chunks: Vec<Result<Bytes, std::io::Error>> = vec![
            Ok(Bytes::from(vec![0u8; 100])),
            Err(std::io::Error::other("test error")),
            Ok(Bytes::from(vec![0u8; 200])),
        ];
        let inner = stream::iter(chunks);

        let mut counting = CountingStream::new(inner, move |n: usize| {
            counter_clone.fetch_add(n as u64, Ordering::Relaxed);
        });

        // First chunk: OK, counted
        assert!(counting.next().await.unwrap().is_ok());
        assert_eq!(counter.load(Ordering::Relaxed), 100);

        // Second chunk: error, NOT counted
        assert!(counting.next().await.unwrap().is_err());
        assert_eq!(counter.load(Ordering::Relaxed), 100);

        // Third chunk: OK, counted
        assert!(counting.next().await.unwrap().is_ok());
        assert_eq!(counter.load(Ordering::Relaxed), 300);
    }

    #[test]
    fn test_estimate_chunks_very_large() {
        // 10 GB file -> 128 MB chunks -> ceil(10240/128) = 80
        let files = vec![("huge.safetensors".to_string(), 10 * 1024 * 1024 * 1024)];
        assert_eq!(estimate_total_chunks(&files, 0), 80);
    }
}
