use bitcoin_test_data::blocks::mainnet_702861;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::{
    fs::{self},
    path::PathBuf,
};

use tx_indexer_primitives::{
    dense::IndexPaths,
    indecies::{BlockTxIndex, ConfirmedTxPtrIndex, InPrevoutIndex, OutSpentByIndex},
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
            || {
                let index_dir = temp_dir("tx_indexer_bench_idx");
                let paths = IndexPaths {
                    txptr: index_dir.join("txptr.idx"),
                    block_tx: index_dir.join("block_tx.idx"),
                    in_prevout: index_dir.join("in_prevout.idx"),
                    out_spent: index_dir.join("out_spent.idx"),
                };
                (paths, index_dir)
            },
            |(paths, index_dir)| {
                let mut parser = Parser::new(&blocks_dir);
                let mut txptr_index = ConfirmedTxPtrIndex::create(&paths.txptr).unwrap();
                let mut block_tx_index = BlockTxIndex::create(&paths.block_tx).unwrap();
                let mut in_prevout_index = InPrevoutIndex::create(&paths.in_prevout).unwrap();
                let mut out_spent_index = OutSpentByIndex::create(&paths.out_spent).unwrap();
                let mut spk_db = SledDBFactory::open(std::env::temp_dir())
                    .unwrap()
                    .spk_db()
                    .unwrap();

                parser
                    .parse_blocks(
                        0..1,
                        &mut txptr_index,
                        &mut block_tx_index,
                        &mut in_prevout_index,
                        &mut out_spent_index,
                        &mut spk_db,
                    )
                    .unwrap();

                let _ = fs::remove_dir_all(&index_dir);
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench_parse_mainnet_702861);
criterion_main!(benches);
