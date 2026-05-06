use std::{path::PathBuf, sync::Arc, time::Instant};

use tx_indexer_fingerprints::HasInputFingerprints;
use tx_indexer_pipeline::{context::PipelineContext, engine::Engine, ops::AllDenseTxs};
use tx_indexer_primitives::{UnifiedStorage, dense::DenseStorageBuilder, test_utils::temp_dir};

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut datadir: Option<PathBuf> = None;
    let mut depth: u32 = 10;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--datadir" => {
                i += 1;
                datadir = Some(PathBuf::from(&args[i]));
            }
            "--depth" => {
                i += 1;
                depth = args[i].parse().unwrap_or_else(|_| {
                    eprintln!("Error: invalid depth: {}", args[i]);
                    std::process::exit(1);
                });
            }
            other => {
                eprintln!("Unknown argument: {other}");
                print_usage();
                std::process::exit(1);
            }
        }
        i += 1;
    }

    let datadir = datadir.unwrap_or_else(|| {
        eprintln!("Error: --datadir is required");
        print_usage();
        std::process::exit(1);
    });

    if !datadir.exists() {
        eprintln!("Error: datadir does not exist: {}", datadir.display());
        std::process::exit(1);
    }

    println!(
        "Indexing last {} blocks (depth: {}) from {}",
        depth + 1,
        depth,
        datadir.display()
    );

    // 1. Build indices into a temp directory
    let out_dir = temp_dir("tx-indexer-example");

    let start = Instant::now();
    let builder = DenseStorageBuilder::sync_from_tip(datadir, out_dir, depth).unwrap_or_else(|e| {
        eprintln!("Error: failed to sync block index: {e}");
        std::process::exit(1);
    });
    let unified: UnifiedStorage = builder.try_into().unwrap_or_else(|e| {
        eprintln!("Error: failed to build indices: {e}");
        std::process::exit(1);
    });
    let index_elapsed = start.elapsed();

    let tx_count = unified.dense_txids_len();
    println!(
        "Indexed {} transactions in {} blocks ({index_elapsed:.2?})",
        tx_count,
        depth + 1,
    );

    // 2. Set up the pipeline
    let ctx = Arc::new(PipelineContext::new());
    let unified = Arc::new(unified);
    let mut engine = Engine::new(ctx.clone(), unified);

    let source = AllDenseTxs::new(&ctx);
    let all_txs = source.txs();
    let rbf_mask = all_txs.filter(|tx_id, ctx| {
        tx_id
            .with(ctx.unified_storage())
            .inputs()
            .any(|input| input.signals_rbf())
    });

    // 3. Evaluate
    let eval_start = Instant::now();
    let result = engine.eval(&rbf_mask).into_owned();
    let eval_elapsed = eval_start.elapsed();

    // 4. Print results
    let rbf_count = result.len();
    let total = engine.eval(&all_txs).into_owned().len();

    println!();
    println!("--- RBF signaling analysis ({eval_elapsed:.2?}) ---");
    println!(
        "Transactions signaling RBF: {rbf_count}/{total} ({:.1}%)",
        if total == 0 {
            0.0
        } else {
            rbf_count as f64 / total as f64 * 100.0
        }
    );
}

fn print_usage() {
    eprintln!("Usage: indexer --datadir <path> [--depth N]");
    eprintln!();
    eprintln!("  --datadir <path>      Bitcoin Core data directory (e.g. ~/.bitcoin/)");
    eprintln!("  --depth N             Index from N blocks before tip to current");
}
