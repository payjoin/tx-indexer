use bitcoin_test_data::blocks::mainnet_702861;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::fs;

use tx_indexer_primitives::{
    indecies::DenseIndexSet,
    parser::Parser,
    sled::db::SledDBFactory,
    test_utils::{temp_dir, write_single_block_file},
};

fn bench_parse_mainnet_702861(c: &mut Criterion) {
    let block_bytes = mainnet_702861();
    let blocks_dir = temp_dir("tx_indexer_bench_blocks");
    write_single_block_file(&blocks_dir, block_bytes).expect("write block file");

    c.bench_function("dense_parse_mainnet_702861", |b| {
        b.iter_batched(
            || temp_dir("tx_indexer_bench_idx"),
            |index_dir| {
                let mut parser = Parser::new(&blocks_dir);
                let mut indices = DenseIndexSet::new(&index_dir).unwrap();
                let mut spk_db = SledDBFactory::open(std::env::temp_dir())
                    .unwrap()
                    .spk_db()
                    .unwrap();

                parser
                    .parse_blocks(0..1, &mut indices, &mut spk_db)
                    .unwrap();

                let _ = fs::remove_dir_all(&index_dir);
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench_parse_mainnet_702861);
criterion_main!(benches);
