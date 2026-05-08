use crate::transaction::Outpoint;
use crate::wallet::WalletId;
use bitcoin::Amount;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UtxoWithMetadata {
    pub(crate) outpoint: Outpoint,
    pub(crate) amount: Amount,
    pub(crate) owner: WalletId,
}

/// Non-committal intent to cospend: every participant coin in the match (no taker/maker roles).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CospendInterest {
    pub(crate) utxos: Vec<UtxoWithMetadata>,
}

#[allow(dead_code)]
fn amount_distance(a: Amount, b: Amount) -> u64 {
    a.to_sat().abs_diff(b.to_sat())
}

/// Returns order book entries sorted by value asymmetry relative to the given reference UTXOs.
/// Each entry is scored by the minimum amount distance to any reference UTXO.
#[allow(dead_code)]
pub(crate) fn generate_candidates(
    order_book: &[UtxoWithMetadata],
    reference_utxos: &[UtxoWithMetadata],
) -> Vec<UtxoWithMetadata> {
    let mut scored: Vec<(u64, &UtxoWithMetadata)> = order_book
        .iter()
        .map(|entry| {
            let min_dist = reference_utxos
                .iter()
                .map(|t| amount_distance(entry.amount, t.amount))
                .min()
                .unwrap_or(u64::MAX);
            (min_dist, entry)
        })
        .collect();

    scored.sort_unstable_by(|(dist_a, a), (dist_b, b)| {
        dist_a
            .cmp(dist_b)
            .then_with(|| a.outpoint.txid.0.cmp(&b.outpoint.txid.0))
            .then_with(|| a.outpoint.index.cmp(&b.outpoint.index))
    });

    scored.into_iter().map(|(_, entry)| entry.clone()).collect()
}
