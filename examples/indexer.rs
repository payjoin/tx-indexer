use std::{path::PathBuf, sync::Arc, time::Instant};

use tx_indexer_heuristics::ast::SignalsRbf;
use tx_indexer_pipeline::{context::PipelineContext, engine::Engine, ops::AllDenseTxs};
use tx_indexer_primitives::{
    UnifiedStorage, dense::IndexPaths, sled::db::SledDBFactory, test_utils::temp_dir,
    unified::DenseBuildSpec,
};

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut blocks_dir: Option<PathBuf> = None;
    let mut range_start: u64 = 0;
    let mut range_end: u64 = 10;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--blocks-dir" => {
                i += 1;
                blocks_dir = Some(PathBuf::from(&args[i]));
            }
            "--range" => {
                i += 1;
                let (start, end) = parse_range(&args[i]);
                range_start = start;
                range_end = end;
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

    if !blocks_dir.exists() {
        eprintln!(
            "Error: blocks directory does not exist: {}",
            blocks_dir.display()
        );
        std::process::exit(1);
    }

    let range = range_start..range_end;
    println!(
        "Indexing blocks {}..{} from {}",
        range_start,
        range_end,
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
    let unified = UnifiedStorage::try_from(DenseBuildSpec {
        blocks_dir: blocks_dir.clone(),
        range: range.clone(),
        paths,
        spk_db,
    })
    .expect("failed to build indices");
    let index_elapsed = start.elapsed();

    let tx_count = unified.dense_txids_len();
    println!(
        "Indexed {} transactions in {} blocks ({index_elapsed:.2?})",
        tx_count,
        range_end - range_start,
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

fn parse_range(s: &str) -> (u64, u64) {
    let parts: Vec<&str> = s.split("..").collect();
    if parts.len() != 2 {
        eprintln!("Error: invalid range format, expected START..END (e.g. 0..10)");
        std::process::exit(1);
    }
    let start: u64 = parts[0].parse().unwrap_or_else(|_| {
        eprintln!("Error: invalid range start: {}", parts[0]);
        std::process::exit(1);
    });
    let end: u64 = parts[1].parse().unwrap_or_else(|_| {
        eprintln!("Error: invalid range end: {}", parts[1]);
        std::process::exit(1);
    });
    (start, end)
}

fn print_usage() {
    eprintln!("Usage: indexer --blocks-dir <path> [--range START..END]");
    eprintln!();
    eprintln!("  --blocks-dir <path>   Directory containing blk*.dat files");
    eprintln!("  --range START..END    Block range to index (default: 0..10)");
}
