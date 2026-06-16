//! Radix CoinJoin primitives vendored from "Small Hamming Weight Denominations
//! for CoinJoins" by Yuval Kogman (@nothingmuch):
//! <https://colab.research.google.com/drive/1We_FvfX_Ob9BapFW3X_By9vTtxUrt3pm>

pub mod analysis;
pub mod denoms;

pub use analysis::{PerSeriesAnalysis, RadixAnalysis, analyze};
