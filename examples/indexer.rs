use std::{path::PathBuf, sync::Arc, time::Instant};

use tx_indexer_heuristics::ast::SignalsRbf;
use tx_indexer_pipeline::{context::PipelineContext, engine::Engine, ops::AllDenseTxs};
use tx_indexer_primitives::{UnifiedStorageBuilder, test_utils::temp_dir};

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut data_dir: Option<PathBuf> = None;
    // Default depth is 10 blocks.
    let mut depth: u32 = 10;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--data-dir" => {
                i += 1;
                data_dir = Some(PathBuf::from(&args[i]));
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

    let data_dir = data_dir.unwrap_or_else(|| {
        eprintln!("Error: --blocks-dir is required");
        print_usage();
        std::process::exit(1);
    });

    if !data_dir.exists() {
        eprintln!(
            "Error: data directory does not exist: {}",
            data_dir.display()
        );
        std::process::exit(1);
    }

    // 1. Build indices into a temp directory
    let tmp = temp_dir("tx-indexer-example");
    let start = Instant::now();
    let unified = UnifiedStorageBuilder::new()
        .with_dense_from_tip(data_dir, depth, tmp.clone())
        .expect("valid paths")
        .build()
        .expect("failed to build indices");
    let index_elapsed = start.elapsed();

    let tx_count = unified.dense_txids_len();
    println!(
        "Indexed {} transactions in {} blocks ({index_elapsed:.2?})",
        tx_count, depth,
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

    // Cleanup
    let _ = std::fs::remove_dir_all(&tmp);
}

fn print_usage() {
    eprintln!("Usage: indexer --data-dir <path> [--depth DEPTH]");
    eprintln!();
    eprintln!("  --data-dir <path>   Directory containing blk*.dat files");
    eprintln!("  --depth DEPTH    Depth to index (default: 10)");
}
