//! Integrity benchmarks: SHA-256, CRC-32C, compression, archive writes.

use criterion::{criterion_group, criterion_main, Criterion};
use reclaim_fabric::archive::{ArchiveBackend, LocalFsArchive};
use reclaim_fabric::compression::{compress_verified_with_bytes, ZstdCodec};
use reclaim_fabric::integrity::{crc32c, ContentHash};

fn sample_data(size: usize) -> Vec<u8> {
    // Repetitive data (compressible) mixed with noise.
    let mut v = Vec::with_capacity(size);
    let mut x = 0x12345678u32;
    while v.len() < size {
        x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        v.extend_from_slice(&x.to_le_bytes());
        v.extend_from_slice(b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    }
    v.truncate(size);
    v
}

fn bench_integrity(c: &mut Criterion) {
    let mut group = c.benchmark_group("integrity");
    let data = sample_data(1 << 20); // 1 MiB

    group.bench_function("sha256_1mib", |b| b.iter(|| ContentHash::of(&data)));
    group.bench_function("crc32c_1mib", |b| b.iter(|| crc32c(&data)));
    group.bench_function("verify_sha256_1mib", |b| {
        let hash = ContentHash::of(&data);
        b.iter(|| reclaim_fabric::integrity::verify_sha256(&data, &hash).unwrap());
    });

    group.bench_function("zstd_compress_1mib", |b| {
        let codec = ZstdCodec::new(3);
        b.iter(|| compress_verified_with_bytes(&codec, &data).unwrap());
    });

    group.bench_function("archive_write_64kib", |b| {
        let dir = tempfile::tempdir().unwrap();
        let archive = LocalFsArchive::new("bench", dir.path()).unwrap();
        let chunk = sample_data(64 * 1024);
        let hash = ContentHash::of(&chunk);
        b.iter(|| archive.write("bench-blob", &chunk, &hash).unwrap());
    });

    group.bench_function("archive_verify_64kib", |b| {
        let dir = tempfile::tempdir().unwrap();
        let archive = LocalFsArchive::new("bench", dir.path()).unwrap();
        let chunk = sample_data(64 * 1024);
        let hash = ContentHash::of(&chunk);
        archive.write("bench-blob", &chunk, &hash).unwrap();
        b.iter(|| archive.verify("bench-blob", &hash).unwrap());
    });

    group.finish();
}

criterion_group!(benches, bench_integrity);
criterion_main!(benches);
