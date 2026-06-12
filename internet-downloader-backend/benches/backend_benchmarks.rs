use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use internet_downloader_backend::utils::file_utils::hash_file;
use internet_downloader_backend::utils::network_utils::BandwidthLimiter;

/// Benchmark the bandwidth limiter's hot path: `register_bytes` is called for
/// every chunk read off the network, so its per-call cost directly impacts
/// throughput on throttled downloads.
fn bench_bandwidth_limiter(c: &mut Criterion) {
    let mut group = c.benchmark_group("bandwidth_limiter");

    // 1 MiB/s limit, the common throttled case where debt accounting runs.
    let limited = BandwidthLimiter::new(1024 * 1024);
    group.bench_function("register_bytes_limited", |b| {
        b.iter(|| limited.register_bytes(black_box(16 * 1024)))
    });

    // Unlimited mode should short-circuit; measure that fast path too.
    let unlimited = BandwidthLimiter::new(0);
    unlimited.set_unlimited(true);
    group.bench_function("register_bytes_unlimited", |b| {
        b.iter(|| unlimited.register_bytes(black_box(16 * 1024)))
    });

    group.finish();
}

/// Benchmark BLAKE3 hashing of whole files, which runs inline during downloads
/// to detect corruption. This is CPU-bound work over the file contents.
fn bench_hash_file(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_file");

    for size in [64 * 1024usize, 1024 * 1024, 8 * 1024 * 1024] {
        // Write a deterministic payload to a temp file once, outside the
        // measured loop, so we only benchmark the hashing itself.
        let mut path = std::env::temp_dir();
        path.push(format!("codspeed_hash_bench_{size}.bin"));

        let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &data).expect("failed to write benchmark fixture");

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &path, |b, path| {
            b.iter(|| hash_file(black_box(path), None).expect("hashing failed"))
        });

        let _ = std::fs::remove_file(&path);
    }

    group.finish();
}

criterion_group!(benches, bench_bandwidth_limiter, bench_hash_file);
criterion_main!(benches);
