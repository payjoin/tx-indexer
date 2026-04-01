use std::{path::PathBuf, sync::Arc, time::Instant};

use tx_indexer_heuristics::ast::SignalsRbf;
use tx_indexer_pipeline::{context::PipelineContext, engine::Engine, ops::AllDenseTxs};
use tx_indexer_primitives::{test_utils::temp_dir, unified::sync_from_tip};

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
        "Indexing {} blocks from tip (datadir: {})",
        depth + 1,
        datadir.display()
    );

    // 1. Build indices into a temp directory (or user-specified index-dir)
    let out_dir = temp_dir("tx-indexer-example");

    let start = Instant::now();
    let unified = sync_from_tip(&datadir, &out_dir, depth).unwrap_or_else(|e| {
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
    let rbf_mask = SignalsRbf::new(all_txs);

    // 3. Evaluate
    let eval_start = Instant::now();
    let result = engine.eval(&rbf_mask);
    let eval_elapsed = eval_start.elapsed();

    // 4. Print results
    let rbf_count = result.values().filter(|&&v| v).count();
    let total = result.len();

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
    eprintln!("  --depth N             Number of blocks before tip to index (default: 10)");
}
