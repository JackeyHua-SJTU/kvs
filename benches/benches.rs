use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use kvs::engine::{KvsEngine, kvs::KvStore, sled::SledKvsEngine};
use rand::prelude::*;
use sled;
use tempfile::TempDir;

fn set_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_bench");
    group.bench_function("kvs", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                (KvStore::open(temp_dir.path()).unwrap(), temp_dir)
            },
            |(mut store, _temp_dir)| {
                for i in 1..(1 << 8) {
                    store.set(format!("key{}", i), "value".to_string()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("sled", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                (
                    SledKvsEngine::open(sled::open(&temp_dir).unwrap()),
                    temp_dir,
                )
            },
            |(mut db, _temp_dir)| {
                for i in 1..(1 << 8) {
                    db.set(format!("key{}", i), "value".to_string()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn get_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_bench");
    for i in &vec![8] {
        group.bench_with_input(format!("kvs_{}", i), i, |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let mut store = KvStore::open(temp_dir.path()).unwrap();
            for key_i in 1..(1 << i) {
                store
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = rand::rng();
            b.iter(|| {
                store
                    .get(format!("key{}", rng.random_range(1..(1 << i))))
                    .unwrap();
            })
        });
    }
    for i in &vec![8] {
        group.bench_with_input(format!("sled_{}", i), i, |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let mut db = SledKvsEngine::open(sled::open(&temp_dir).unwrap());
            for key_i in 1..(1 << i) {
                db.set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = rand::rng();
            b.iter(|| {
                db.get(format!("key{}", rng.random_range(1..(1 << i))))
                    .unwrap();
            })
        });
    }
    group.finish();
}

criterion_group!(benches, set_bench, get_bench);
criterion_main!(benches);
