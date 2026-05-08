use std::env;
use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
struct Args {
    /// Directory to save graph, results JSON, and config.
    #[arg(long, value_name = "DIR")]
    artifacts_dir: Option<PathBuf>,
}

fn main() {
    env_logger::init();
    let args = Args::parse();

    // Read config file path from environment or use default
    let config_path = env::var("CONFIG_FILE").unwrap_or_else(|_| "config.toml".to_string());

    let config = btsim::config::Config::from_file(&config_path)
        .unwrap_or_else(|_| panic!("Failed to parse config file: {}", config_path));

    let seed = config.simulation.seed.unwrap_or(42);
    let mut sim = btsim::SimulationBuilder::new(
        seed,
        config.wallet_types,
        config.simulation.max_timestep,
        1, // TODO: hardcoded block interval for now. If we change this we need to ensure payment obligations are not being double handled.
        config.simulation.num_payment_obligations,
    )
    .build();

    sim.build_universe();
    let result = sim.run();
    if let Some(dir) = args.artifacts_dir.as_ref() {
        std::fs::create_dir_all(dir).unwrap();
        let graph_path = dir.join("graph.svg");
        let results_path = dir.join("results.json");
        let config_out_path = dir.join("config.toml");
        result.save_tx_graph(graph_path);
        result.save_results_json(results_path);
        std::fs::copy(&config_path, config_out_path).unwrap();
    } else {
        result.save_tx_graph("graph.svg");
    }
    println!(
        "Total payment obligations: {}",
        result.total_payment_obligations()
    );
    println!(
        "Missed payment obligations percentage: {:?}",
        result.percentage_of_payment_obligations_missed()
    );
    println!(
        "Total block weight used (wu): {}",
        result.total_block_weight()
    );
    println!(
        "Average fee cost (sats): {}",
        result.average_fee_cost().to_sat()
    );
}
