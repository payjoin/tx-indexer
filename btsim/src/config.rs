use bitcoin::Amount;
use serde::Deserialize;
use std::fs;

use crate::metrics::PrivacyBundle;
use crate::script_type::ScriptType;

fn default_subset_sum_max_size() -> usize {
    6
}

fn default_brute_max_terms() -> usize {
    15
}

fn default_radix_density_floor() -> f64 {
    0.5
}

pub(crate) fn build_privacy_bundle(scorer: &ScorerConfig) -> PrivacyBundle {
    let mut metrics: Vec<Box<dyn crate::metrics::ErasedPrivacyMetric>> = vec![];
    if let Some(threshold) = scorer.subset_sum_threshold {
        metrics.push(Box::new(crate::lower_bound_metric::WLowerBoundMetric {
            max_size: scorer.subset_sum_max_size,
            brute_max_terms: scorer.brute_max_terms,
            threshold: u128::from(threshold),
        }));
    }
    if let Some(radix_threshold) = scorer.radix_threshold {
        metrics.push(Box::new(crate::lower_bound_metric::RadixMappingMetric {
            max_size: scorer.subset_sum_max_size,
            threshold: u128::from(radix_threshold),
            density_floor: scorer.radix_density_floor,
        }));
    }
    PrivacyBundle {
        metrics,
        budget: Amount::from_sat(scorer.privacy_weight as u64),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub simulation: SimulationConfig,
    pub wallet_types: Vec<WalletTypeConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SimulationConfig {
    pub seed: Option<u64>,
    pub max_timestep: u64,
    pub num_payment_obligations: usize,
    #[serde(default)]
    pub denominate_change: bool,
    #[serde(default)]
    pub denominated_funding: Option<DenominatedFunding>,
}

/// Endows each wallet with `utxos_per_wallet` standard-denomination UTXOs drawn from the narrow band
/// `[band_min, band_max]`, instead of the default ~50 BTC coinbase — so a multiparty coinjoin of them
/// is dense (small κ). `None` = default funding.
#[derive(Debug, Clone, Deserialize)]
pub struct DenominatedFunding {
    pub band_min: u64,
    pub band_max: u64,
    pub utxos_per_wallet: usize,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct WalletTypeConfig {
    pub name: String,
    pub count: usize,
    pub strategies: Vec<String>,
    pub scorer: ScorerConfig,
    #[serde(default)]
    pub script_type: ScriptType,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ScorerConfig {
    /// Weight applied to privacy score
    pub privacy_weight: f64,
    /// Weight applied to deadline urgency for payment obligations
    pub payment_obligation_weight: f64,
    /// Minimum number of viable unilateral fallback plans required before committing to a
    /// multiparty session. 0 = no restriction (default).
    #[serde(default)]
    pub min_fallback_plans: usize,
    /// Penalize plans whose best subset-sum lower bound is below this. `None` = metric inactive.
    #[serde(default)]
    pub subset_sum_threshold: Option<u64>,
    #[serde(default = "default_subset_sum_max_size")]
    pub subset_sum_max_size: usize,
    #[serde(default = "default_brute_max_terms")]
    pub brute_max_terms: usize,
    /// Credit radix k×m! mappings below this. `None` = radix metric inactive.
    #[serde(default)]
    pub radix_threshold: Option<u64>,
    /// Minimum `radix_density` of outputs for the radix metric to credit (gate). Default 0.5.
    #[serde(default = "default_radix_density_floor")]
    pub radix_density_floor: f64,
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = fs::read_to_string(path)?;
        let config: Config = toml::from_str(&contents)?;

        // Validate strategy names
        let valid_strategies = [
            "UnilateralSpender",
            "Consolidator",
            "BatchSpender",
            "MultipartyStrategy",
            "AggregatorStrategy",
        ];
        for wallet_type in &config.wallet_types {
            for strategy in &wallet_type.strategies {
                if !valid_strategies.contains(&strategy.as_str()) {
                    return Err(format!(
                        "Invalid strategy name: {}. Valid strategies are: {:?}",
                        strategy, valid_strategies
                    )
                    .into());
                }
            }
            if wallet_type.count == 0 {
                return Err(
                    format!("Wallet type '{}' must have count > 0", wallet_type.name).into(),
                );
            }
        }

        Ok(config)
    }

    pub fn total_wallets(&self) -> usize {
        self.wallet_types.iter().map(|wt| wt.count).sum()
    }
}

#[cfg(test)]
mod bundle_tests {
    use super::*;

    fn cfg(threshold: Option<u64>) -> ScorerConfig {
        ScorerConfig {
            privacy_weight: 1000.0,
            payment_obligation_weight: 0.0,
            min_fallback_plans: 0,
            subset_sum_threshold: threshold,
            subset_sum_max_size: 6,
            brute_max_terms: 15,
            radix_threshold: None,
            radix_density_floor: 0.5,
        }
    }

    #[test]
    fn no_threshold_yields_empty_bundle() {
        assert_eq!(
            crate::config::build_privacy_bundle(&cfg(None))
                .metrics
                .len(),
            0
        );
    }

    #[test]
    fn threshold_yields_one_metric() {
        assert_eq!(
            crate::config::build_privacy_bundle(&cfg(Some(1000)))
                .metrics
                .len(),
            1
        );
    }

    #[test]
    fn threshold_parses_from_toml_and_activates_metric() {
        let toml = r#"
            privacy_weight = 1000.0
            payment_obligation_weight = 1.0
            subset_sum_threshold = 100
        "#;
        let scorer: ScorerConfig = toml::from_str(toml).expect("ScorerConfig must parse from TOML");
        assert_eq!(scorer.subset_sum_threshold, Some(100));
        assert_eq!(build_privacy_bundle(&scorer).metrics.len(), 1);
    }

    #[test]
    fn radix_threshold_parses_and_adds_a_second_metric() {
        let toml = r#"
            privacy_weight = 1000.0
            payment_obligation_weight = 1.0
            subset_sum_threshold = 100
            radix_threshold = 50
        "#;
        let scorer: ScorerConfig = toml::from_str(toml).expect("parses");
        assert_eq!(scorer.radix_threshold, Some(50));
        assert_eq!(scorer.radix_density_floor, 0.5); // default
        assert_eq!(build_privacy_bundle(&scorer).metrics.len(), 2); // W + radix
    }

    #[test]
    fn radix_threshold_alone_yields_one_metric() {
        let scorer = ScorerConfig {
            privacy_weight: 1000.0,
            payment_obligation_weight: 0.0,
            min_fallback_plans: 0,
            subset_sum_threshold: None,
            subset_sum_max_size: 6,
            brute_max_terms: 15,
            radix_threshold: Some(10),
            radix_density_floor: 0.5,
        };
        assert_eq!(build_privacy_bundle(&scorer).metrics.len(), 1); // radix only
    }
}
