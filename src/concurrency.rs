//! Memory-aware concurrency planning for hfs3.
//!
//! Reads /proc/meminfo to determine available system memory, then computes
//! an adaptive transfer plan: chunk size based on largest file, and max
//! concurrent transfers based on available RAM.
//!
//! All computation functions are pure and testable without /proc/meminfo.
//! Use `plan_transfer_with_memory` for deterministic testing.

use crate::error::Hfs3Error;

const MB: usize = 1024 * 1024;
const GB: u64 = 1024 * 1024 * 1024;

/// Transfer plan computed from file sizes and available system memory.
#[derive(Debug, Clone)]
pub struct TransferPlan {
    /// Max files to transfer in parallel.
    pub max_concurrent: usize,
    /// Bytes per S3 multipart part (memory-aware).
    pub chunk_size: usize,
    /// Max S3 parts uploading concurrently within a single file.
    pub max_parts_in_flight: usize,
    /// Bytes available from /proc/meminfo.
    pub available_memory: u64,
}

/// Baseline chunk size based on file size alone (no memory awareness).
///   < 1 GB  ->  8 MB chunks
///   < 5 GB  -> 64 MB chunks
///   >= 5 GB -> 128 MB chunks
///
/// Prefer `chunk_size_for_transfer` when available memory is known.
pub fn chunk_size_for_file(file_size: u64) -> usize {
    if file_size < GB {
        8 * MB
    } else if file_size < 5 * GB {
        64 * MB
    } else {
        128 * MB
    }
}

/// Memory-aware chunk size: uses both file size and available RAM.
///
/// When RAM is plentiful, uses larger chunks to reduce S3 API round-trips.
/// Also enforces the S3 10,000-part limit for very large files.
pub fn chunk_size_for_transfer(file_size: u64, available_memory: u64) -> usize {
    let base = chunk_size_for_file(file_size);

    let mem_floor = if available_memory >= 32 * GB {
        256 * MB
    } else if available_memory >= 16 * GB {
        128 * MB
    } else if available_memory >= 8 * GB {
        64 * MB
    } else {
        0
    };

    // S3 allows max 10,000 parts per multipart upload
    let s3_min = if file_size > 0 {
        let min_bytes = file_size.div_ceil(10_000) as usize;
        min_bytes.div_ceil(MB) * MB // round up to nearest MB
    } else {
        0
    };

    base.max(mem_floor).max(s3_min)
}

/// Max concurrent S3 part uploads within a single file transfer.
///
/// Budget: use at most 1/3 of available RAM for in-flight part buffers,
/// divided across actual concurrent file transfers. Clamped to 1..=8.
pub fn calculate_max_parts(
    chunk_size: usize,
    available_memory: u64,
    file_count: usize,
    max_concurrent: usize,
) -> usize {
    if chunk_size == 0 {
        return 1;
    }
    let actual_concurrent = file_count.min(max_concurrent).max(1) as u64;
    let budget = available_memory / 3;
    let per_file = budget / actual_concurrent;
    let parts = per_file / chunk_size as u64;
    (parts as usize).clamp(1, 8)
}

/// Read MemAvailable from /proc/meminfo.
/// Returns bytes (the file reports kB, multiply by 1024).
pub fn available_memory_bytes() -> Result<u64, Hfs3Error> {
    let contents = std::fs::read_to_string("/proc/meminfo").map_err(Hfs3Error::Io)?;

    for line in contents.lines() {
        if line.starts_with("MemAvailable:") {
            // Format: "MemAvailable:   12345678 kB"
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let kb: u64 = parts[1]
                    .parse()
                    .map_err(|_| Hfs3Error::Parse("Failed to parse MemAvailable".into()))?;
                tracing::info!(
                    mem_available_kb = kb,
                    mem_available_bytes = kb * 1024,
                    "Read /proc/meminfo"
                );
                return Ok(kb * 1024);
            }
        }
    }
    Err(Hfs3Error::Parse(
        "MemAvailable not found in /proc/meminfo".into(),
    ))
}

/// Calculate max concurrent file transfers.
///
/// Assumes each concurrent transfer buffers ~2 chunks in flight
/// (one being uploaded, one being filled from download).
///
/// Formula: available_memory / (chunk_size * 2)
/// Floor: 2 (always allow at least 2 concurrent transfers)
/// Cap: 32 (avoid overwhelming the network/S3)
pub fn calculate_max_concurrency(chunk_size: usize, available_memory: u64) -> usize {
    let bytes_per_transfer = (chunk_size as u64) * 2;
    if bytes_per_transfer == 0 {
        return 2;
    }
    let calculated = (available_memory / bytes_per_transfer) as usize;
    calculated.clamp(2, 32)
}

/// Compute a transfer plan from file list and system memory.
///
/// Uses the largest file to determine chunk size (conservative),
/// then computes concurrency from available RAM.
pub fn plan_transfer(files: &[(&str, u64)]) -> Result<TransferPlan, Hfs3Error> {
    let available = available_memory_bytes()?;
    let max_file_size = files.iter().map(|(_, s)| *s).max().unwrap_or(0);
    let chunk_size = chunk_size_for_transfer(max_file_size, available);
    let max_concurrent = calculate_max_concurrency(chunk_size, available);
    let max_parts = calculate_max_parts(chunk_size, available, files.len(), max_concurrent);

    tracing::info!(
        available_memory_mb = available / (1024 * 1024),
        chunk_size_mb = chunk_size / (1024 * 1024),
        max_concurrent,
        max_parts_in_flight = max_parts,
        file_count = files.len(),
        largest_file_mb = max_file_size / (1024 * 1024),
        "Transfer plan computed"
    );

    Ok(TransferPlan {
        max_concurrent,
        chunk_size,
        max_parts_in_flight: max_parts,
        available_memory: available,
    })
}

/// Same as plan_transfer but accepts pre-computed available memory.
/// Used in tests and when /proc/meminfo is unavailable.
pub fn plan_transfer_with_memory(files: &[(&str, u64)], available_memory: u64) -> TransferPlan {
    let max_file_size = files.iter().map(|(_, s)| *s).max().unwrap_or(0);
    let chunk_size = chunk_size_for_transfer(max_file_size, available_memory);
    let max_concurrent = calculate_max_concurrency(chunk_size, available_memory);
    let max_parts = calculate_max_parts(chunk_size, available_memory, files.len(), max_concurrent);

    tracing::info!(
        available_memory_mb = available_memory / (1024 * 1024),
        chunk_size_mb = chunk_size / (1024 * 1024),
        max_concurrent,
        max_parts_in_flight = max_parts,
        file_count = files.len(),
        largest_file_mb = max_file_size / (1024 * 1024),
        "Transfer plan computed"
    );

    TransferPlan {
        max_concurrent,
        chunk_size,
        max_parts_in_flight: max_parts,
        available_memory,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_max_concurrency_normal() {
        // 16 GB available, 8 MB chunks
        // 16*1024*1024*1024 / (8*1024*1024 * 2) = 1024 -> capped at 32
        let result = calculate_max_concurrency(8 * 1024 * 1024, 16 * 1024 * 1024 * 1024);
        assert_eq!(result, 32);
    }

    #[test]
    fn test_calculate_max_concurrency_low_memory() {
        // 100 MB available, 64 MB chunks
        // 100*1024*1024 / (64*1024*1024 * 2) = 0 -> floored at 2
        let result = calculate_max_concurrency(64 * 1024 * 1024, 100 * 1024 * 1024);
        assert_eq!(result, 2);
    }

    #[test]
    fn test_calculate_max_concurrency_moderate() {
        // 1 GB available, 8 MB chunks
        // 1024*1024*1024 / (8*1024*1024 * 2) = 64 -> capped at 32
        let result = calculate_max_concurrency(8 * 1024 * 1024, 1024 * 1024 * 1024);
        assert_eq!(result, 32);
    }

    #[test]
    fn test_calculate_max_concurrency_small_memory() {
        // 50 MB available, 8 MB chunks
        // 50*1024*1024 / (8*1024*1024 * 2) = 3
        let result = calculate_max_concurrency(8 * 1024 * 1024, 50 * 1024 * 1024);
        assert_eq!(result, 3);
    }

    #[test]
    fn test_calculate_max_concurrency_floor() {
        // 1 MB available, 8 MB chunks -> 0, floored to 2
        let result = calculate_max_concurrency(8 * 1024 * 1024, 1024 * 1024);
        assert_eq!(result, 2);
    }

    #[test]
    fn test_calculate_max_concurrency_zero_chunk() {
        // Edge case: 0 chunk size -> floor at 2
        let result = calculate_max_concurrency(0, 1024 * 1024 * 1024);
        assert_eq!(result, 2);
    }

    #[test]
    fn test_plan_transfer_with_memory() {
        let files = vec![
            ("small.txt", 1024u64),
            ("big.bin", 500 * 1024 * 1024), // 500 MB
        ];
        let plan = plan_transfer_with_memory(&files, 1024 * 1024 * 1024); // 1 GB
                                                                          // 1 GB RAM < 8 GB → no memory floor, uses file-size tier: 8 MB
        assert_eq!(plan.chunk_size, 8 * 1024 * 1024);
        assert!(plan.max_concurrent >= 2);
        assert!(plan.max_concurrent <= 32);
        assert!(plan.max_parts_in_flight >= 1);
    }

    #[test]
    fn test_plan_transfer_with_large_files() {
        let files = vec![
            ("huge.safetensors", 6 * 1024 * 1024 * 1024u64), // 6 GB
        ];
        let plan = plan_transfer_with_memory(&files, 32 * 1024 * 1024 * 1024); // 32 GB
                                                                               // 32 GB RAM → memory floor 256 MB, file tier 128 MB → max = 256 MB
        assert_eq!(plan.chunk_size, 256 * 1024 * 1024);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_available_memory_bytes_reads_procfs() {
        // On Linux, /proc/meminfo should exist and return > 0
        let mem = available_memory_bytes().unwrap();
        assert!(mem > 0, "MemAvailable should be positive");
        // Sanity: should be less than 1 TB
        assert!(mem < 1024 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_chunk_size_for_file_small() {
        // < 1 GB -> 8 MB
        assert_eq!(chunk_size_for_file(500 * 1024 * 1024), 8 * MB);
        assert_eq!(chunk_size_for_file(1024), 8 * MB);
    }

    #[test]
    fn test_chunk_size_for_file_medium() {
        // 1 GB <= size < 5 GB -> 64 MB
        assert_eq!(chunk_size_for_file(GB), 64 * MB);
        assert_eq!(chunk_size_for_file(2 * GB), 64 * MB);
    }

    #[test]
    fn test_chunk_size_for_file_large() {
        // >= 5 GB -> 128 MB
        assert_eq!(chunk_size_for_file(5 * GB), 128 * MB);
        assert_eq!(chunk_size_for_file(10 * GB), 128 * MB);
    }

    // --- Memory-aware chunk sizing tests ---

    #[test]
    fn test_chunk_size_for_transfer_low_ram() {
        // 4 GB RAM → no memory floor
        let cs = chunk_size_for_transfer(500 * 1024 * 1024, 4 * GB);
        assert_eq!(cs, 8 * MB); // file-size tier only
    }

    #[test]
    fn test_chunk_size_for_transfer_8gb_ram() {
        // 8 GB RAM → floor 64 MB, file tier for 500MB = 8 MB → 64 MB
        let cs = chunk_size_for_transfer(500 * 1024 * 1024, 8 * GB);
        assert_eq!(cs, 64 * MB);
    }

    #[test]
    fn test_chunk_size_for_transfer_16gb_ram() {
        // 16 GB RAM → floor 128 MB, file tier for 500MB = 8 MB → 128 MB
        let cs = chunk_size_for_transfer(500 * 1024 * 1024, 16 * GB);
        assert_eq!(cs, 128 * MB);
    }

    #[test]
    fn test_chunk_size_for_transfer_32gb_ram() {
        // 32 GB RAM → floor 256 MB, file tier for 2GB = 64 MB → 256 MB
        let cs = chunk_size_for_transfer(2 * GB, 32 * GB);
        assert_eq!(cs, 256 * MB);
    }

    #[test]
    fn test_chunk_size_for_transfer_49gb_ram() {
        // 49 GB RAM → floor 256 MB
        let ram = 49 * GB;
        // Small file: max(8 MB, 256 MB) = 256 MB
        assert_eq!(chunk_size_for_transfer(100 * 1024 * 1024, ram), 256 * MB);
        // 5 GB file: max(128 MB, 256 MB) = 256 MB
        assert_eq!(chunk_size_for_transfer(5 * GB, ram), 256 * MB);
    }

    #[test]
    fn test_chunk_size_for_transfer_file_tier_wins() {
        // 10 GB RAM → floor 64 MB, file tier for 6 GB = 128 MB → 128 MB
        let cs = chunk_size_for_transfer(6 * GB, 10 * GB);
        assert_eq!(cs, 128 * MB);
    }

    #[test]
    fn test_chunk_size_for_transfer_s3_10k_limit() {
        // 5 TB file with low RAM → S3 limit forces larger chunks
        // 5 TB / 10,000 = 512 MB (rounded up to MB boundary)
        let five_tb = 5 * 1024 * GB;
        let cs = chunk_size_for_transfer(five_tb, 2 * GB);
        assert!(
            cs >= 512 * MB,
            "chunk must be >= 512 MB for 5 TB file, got {}",
            cs / MB
        );
    }

    // --- Concurrent parts tests ---

    #[test]
    fn test_calculate_max_parts_single_large_file() {
        // 49 GB RAM, 256 MB chunks, 1 file, max_concurrent=32
        // actual_concurrent = min(1, 32) = 1
        // budget = 49/3 ≈ 16.3 GB, per_file = 16.3 GB
        // parts = 16.3 GB / 256 MB = 65 → capped at 8
        let parts = calculate_max_parts(256 * MB, 49 * GB, 1, 32);
        assert_eq!(parts, 8);
    }

    #[test]
    fn test_calculate_max_parts_many_files() {
        // 16 GB RAM, 128 MB chunks, 50 files, max_concurrent=32
        // actual_concurrent = 32
        // budget = 16/3 ≈ 5.3 GB, per_file = 5.3 GB / 32 ≈ 170 MB
        // parts = 170 MB / 128 MB = 1
        let parts = calculate_max_parts(128 * MB, 16 * GB, 50, 32);
        assert_eq!(parts, 1);
    }

    #[test]
    fn test_calculate_max_parts_low_ram() {
        // 2 GB RAM, 8 MB chunks, 10 files, max_concurrent=10
        // budget = 2/3 ≈ 682 MB, per_file = 68 MB
        // parts = 68 MB / 8 MB = 8
        let parts = calculate_max_parts(8 * MB, 2 * GB, 10, 10);
        assert_eq!(parts, 8);
    }

    #[test]
    fn test_calculate_max_parts_floor() {
        // Very low RAM, big chunks → floor at 1
        let parts = calculate_max_parts(256 * MB, 100 * 1024 * 1024, 10, 10);
        assert_eq!(parts, 1);
    }

    #[test]
    fn test_calculate_max_parts_zero_chunk() {
        let parts = calculate_max_parts(0, 32 * GB, 10, 32);
        assert_eq!(parts, 1);
    }

    // --- Plan integration tests ---

    #[test]
    fn test_plan_high_ram_single_file() {
        // Simulates user's scenario: 49GB RAM, 1 large file
        let files = vec![("model.safetensors", 10 * GB)];
        let plan = plan_transfer_with_memory(&files, 49 * GB);

        assert_eq!(plan.chunk_size, 256 * MB);
        assert_eq!(plan.max_parts_in_flight, 8);
        assert!(plan.max_concurrent >= 2);
    }

    #[test]
    fn test_plan_high_ram_many_small_files() {
        let files: Vec<(&str, u64)> = (0..100)
            .map(|_| ("file.bin", 50 * 1024 * 1024u64))
            .collect();
        let plan = plan_transfer_with_memory(&files, 49 * GB);

        assert_eq!(plan.chunk_size, 256 * MB);
        assert_eq!(plan.max_concurrent, 32);
        // 100 files, 32 concurrent → per_file budget = 16.3GB/32 ≈ 522MB
        // 522 MB / 256 MB = 2
        assert_eq!(plan.max_parts_in_flight, 2);
    }
}
