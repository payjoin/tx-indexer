use std::collections::HashMap;

use tx_indexer_fingerprints::{
    classify::classify_script_pubkey,
    input::HasInputFingerprints,
    input_with_prevout::{has_uncompressed_pubkey, taproot_keyspend_non_default_sighash},
    transaction::{
        address_reuse, anti_fee_snipe, bip68_with_absolute_locktime, input_order,
        mixed_input_types, nlocktime_optin_without_use, output_structure,
    },
    types::{InputSortingType, OutputStructureType},
};
use tx_indexer_pipeline::{
    engine::EvalContext,
    expr::Expr,
    node::{Node, NodeId},
    value::{Mask, NormalizedFingerprints, TxSet},
};
use tx_indexer_primitives::{AbstractTransaction, AbstractTxIn, HasScriptPubkey, unified::AnyTxId};

/// Node that detects transactions signaling opt-in RBF.
///
/// A transaction signals RBF if any of its inputs has sequence < 0xfffffffe (BIP 125).
/// TODO: this is scaffolding. In the pipeline we should just be able to use map/reduce semantics for any fingerprint. Not build custom nodes for each fingerprint.
pub struct SignalsRbfNode {
    input: Expr<TxSet>,
}

impl SignalsRbfNode {
    pub fn new(input: Expr<TxSet>) -> Self {
        Self { input }
    }
}

impl Node for SignalsRbfNode {
    type OutputValue = Mask<AnyTxId>;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> HashMap<AnyTxId, bool> {
        let tx_ids = ctx.get(&self.input);
        tx_ids
            .iter()
            .map(|tx_id| {
                let tx = tx_id.with(ctx.unified_storage());
                let any_rbf = tx.inputs().any(|input| input.signals_rbf());
                (*tx_id, any_rbf)
            })
            .collect()
    }

    fn name(&self) -> &'static str {
        "SignalsRbf"
    }
}

pub struct SignalsRbf;

impl SignalsRbf {
    pub fn new(input: Expr<TxSet>) -> Expr<Mask<AnyTxId>> {
        let ctx = input.context().clone();
        ctx.register(SignalsRbfNode::new(input))
    }
}

fn sorted_deduped(vals: impl Iterator<Item = u32>) -> Vec<u32> {
    let mut v: Vec<u32> = vals.collect();
    v.sort_unstable();
    v.dedup();
    v
}

pub struct CollectFingerprintsNode {
    input: Expr<TxSet>,
}

impl CollectFingerprintsNode {
    pub fn new(input: Expr<TxSet>) -> Self {
        Self { input }
    }
}

impl Node for CollectFingerprintsNode {
    type OutputValue = NormalizedFingerprints;

    fn dependencies(&self) -> Vec<NodeId> {
        vec![self.input.id()]
    }

    fn evaluate(&self, ctx: &EvalContext) -> Vec<Vec<u32>> {
        let tx_ids = ctx.get(&self.input);
        let storage = ctx.unified_storage();
        let mut fingerprints = Vec::new();

        tx_ids.iter().for_each(|tx_id| {
            let tx = tx_id.with(storage);
            let mut f = vec![];

            // [0] signals_rbf
            f.push(tx.inputs().any(|input| input.signals_rbf()) as u32);
            // [1] low_r_grinding
            f.push(tx.inputs().any(|input| input.low_r_grinding()) as u32);

            let inputs: Vec<_> = tx.inputs().collect();
            let locktime = tx.locktime();

            // [2] anti_fee_snipe
            f.push(anti_fee_snipe(locktime) as u32);
            // [3] nlocktime_optin_without_use
            f.push(nlocktime_optin_without_use(&inputs, locktime) as u32);
            // [4] bip68_with_absolute_locktime
            f.push(bip68_with_absolute_locktime(&inputs, locktime) as u32);

            // For dense (confirmed) txs, add fingerprints that need raw bitcoin types
            if let Some(btx) = storage.get_bitcoin_tx(*tx_id) {
                // output_types — sorted deduped discriminants
                let output_types = sorted_deduped(
                    btx.output
                        .iter()
                        .map(|o| classify_script_pubkey(&o.script_pubkey.to_bytes()).as_u32()),
                );
                f.extend(output_types);

                // output_structure — sorted deduped discriminants
                let structure_types = sorted_deduped(
                    output_structure(&btx.output)
                        .into_iter()
                        .map(OutputStructureType::as_u32),
                );
                f.extend(structure_types);

                // Collect prevout TxOuts for inputs (requires loading spent txs)
                let prevouts: Vec<bitcoin::TxOut> = inputs
                    .iter()
                    .filter_map(|input| {
                        let prevout_id = input.prev_txout_id()?;
                        storage.get_bitcoin_txout(prevout_id)
                    })
                    .collect();

                // Only add prevout-based fingerprints when all prevouts resolved
                if prevouts.len() == inputs.len() {
                    // input_type — sorted deduped output types of prevout scripts
                    let input_types = sorted_deduped(
                        prevouts
                            .iter()
                            .map(|o| classify_script_pubkey(&o.script_pubkey_bytes()).as_u32()),
                    );
                    f.extend(input_types);

                    // mixed_input_types
                    f.push(mixed_input_types(&prevouts) as u32);

                    // address_reuse
                    f.push(address_reuse(&btx.output, &prevouts) as u32);

                    // input_order — sorted deduped discriminants
                    let order_types = sorted_deduped(
                        input_order(&btx.input, &prevouts)
                            .into_iter()
                            .map(InputSortingType::as_u32),
                    );
                    f.extend(order_types);

                    // has_uncompressed_pubkey — any input with uncompressed pubkey
                    f.push(inputs.iter().zip(prevouts.iter()).any(|(inp, prevout)| {
                        has_uncompressed_pubkey(inp, prevout)
                    }) as u32);

                    // taproot_keyspend_non_default_sighash — any input with explicit sighash in taproot keyspend
                    f.push(inputs.iter().zip(prevouts.iter()).any(|(inp, prevout)| {
                        taproot_keyspend_non_default_sighash(inp, prevout)
                    }) as u32);
                }
            }

            fingerprints.push(f);
        });
        fingerprints
    }

    fn name(&self) -> &'static str {
        "CollectFingerprints"
    }
}

pub struct CollectFingerprints;

impl CollectFingerprints {
    pub fn new(input: Expr<TxSet>) -> Expr<NormalizedFingerprints> {
        let ctx = input.context().clone();
        ctx.register(CollectFingerprintsNode::new(input))
    }
}
