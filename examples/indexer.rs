use std::{path::PathBuf, sync::Arc, time::Instant};

use tx_indexer_heuristics::ast::SignalsRbf;
use tx_indexer_pipeline::{context::PipelineContext, engine::Engine, ops::AllDenseTxs};
use tx_indexer_primitives::{
    dense::IndexPaths, sled::db::SledDBFactory, test_utils::temp_dir, unified::sync_from_tip,
};

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut blocks_dir: Option<PathBuf> = None;
    let mut index_dir: Option<PathBuf> = None;
    let mut depth: u32 = 10;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--blocks-dir" => {
                i += 1;
                blocks_dir = Some(PathBuf::from(&args[i]));
            }
            "--index-dir" => {
                i += 1;
                index_dir = Some(PathBuf::from(&args[i]));
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

    let blocks_dir = blocks_dir.unwrap_or_else(|| {
        eprintln!("Error: --blocks-dir is required");
        print_usage();
        std::process::exit(1);
    });

    let index_dir = index_dir.unwrap_or_else(|| {
        eprintln!("Error: --index-dir is required");
        print_usage();
        std::process::exit(1);
    });

    if !blocks_dir.exists() {
        eprintln!(
            "Error: blocks directory does not exist: {}",
            blocks_dir.display()
        );
        std::process::exit(1);
    }

    if !index_dir.exists() {
        eprintln!(
            "Error: index directory does not exist: {}",
            index_dir.display()
        );
        std::process::exit(1);
    }

    println!(
        "Indexing {} blocks from tip (blocks-dir: {})",
        depth + 1,
        blocks_dir.display()
    );

    // 1. Build indices into a temp directory
    let tmp = temp_dir("tx-indexer-example");
    let paths = IndexPaths {
        txptr: tmp.join("txptr.bin"),
        block_tx: tmp.join("block_tx.bin"),
        in_prevout: tmp.join("in_prevout.bin"),
        out_spent: tmp.join("out_spent.bin"),
    };

    let spk_db = SledDBFactory::open(tmp.join("spk_db"))
        .expect("failed to open sled DB")
        .spk_db()
        .expect("failed to open spk_db tree");

    let start = Instant::now();
    let unified = sync_from_tip(&blocks_dir, &index_dir, depth, paths, spk_db)
        .expect("failed to build indices");
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

    // Cleanup
    let _ = std::fs::remove_dir_all(&tmp);
}

fn print_usage() {
    eprintln!("Usage: indexer --blocks-dir <path> --index-dir <path> [--depth N]");
    eprintln!();
    eprintln!("  --blocks-dir <path>   Directory containing blk*.dat files");
    eprintln!("  --index-dir <path>    Path to Bitcoin Core's blocks/index LevelDB");
    eprintln!("  --depth N             Number of blocks before tip to index (default: 10)");
}
