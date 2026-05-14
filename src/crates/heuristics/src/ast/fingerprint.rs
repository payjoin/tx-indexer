use tx_indexer_fingerprints::{
    InputSortingType, classify_script_pubkey,
    input::HasInputFingerprints,
    input_with_prevout::{has_uncompressed_pubkey, taproot_keyspend_non_default_sighash},
    transaction::{
        address_reuse, anti_fee_snipe, bip68_with_absolute_locktime, input_order, is_bip69_sorted,
        mixed_input_types, nlocktime_optin_without_use, output_structure,
    },
};
use tx_indexer_pipeline::{
    engine::EvalContext,
    expr::Expr,
    node::{Node, NodeId},
    value::{NormalizedFingerprints, TxSet},
};
use tx_indexer_primitives::{HasScriptPubkey, handle::TxOutHandle, traits::HasNLockTime};

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
    #[allow(dead_code)]
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

            // signals_rbf
            f.push(tx.inputs().any(|input| input.signals_rbf()) as u32);
            // low_r_grinding
            f.push(tx.inputs().any(|input| input.low_r_grinding()) as u32);

            let inputs: Vec<_> = tx.inputs().collect();
            let outputs: Vec<_> = tx.outputs().collect();
            let locktime = tx.locktime();

            // anti_fee_snipe
            f.push(anti_fee_snipe(locktime) as u32);
            // nlocktime_optin_without_use
            f.push(nlocktime_optin_without_use(&inputs, locktime) as u32);
            // bip68_with_absolute_locktime
            f.push(bip68_with_absolute_locktime(&inputs, locktime) as u32);

            // For dense (confirmed) txs, add fingerprints that need raw bitcoin types
            // output_types - sorted deduped discriminants
            let output_types = sorted_deduped(
                outputs
                    .iter()
                    .map(|o| classify_script_pubkey(&o.script_pubkey_bytes()).as_u32()),
            );
            f.extend(output_types);

            // output_structure - sorted deduped discriminants
            f.push(output_structure(&outputs).as_u32());

            // Is outputs bip69 sorted
            f.push(is_bip69_sorted(&outputs) as u32);

            // Collect prevout TxOuts for inputs (requires loading spent txs)
            let prevouts: Vec<TxOutHandle<'_>> = inputs
                .iter()
                .map(|input| {
                    input
                        .prev_txout()
                        .expect("Prevout should always be present for non pruned setup")
                })
                .collect();

            // input_type - sorted deduped output types of prevout scripts
            let input_types = sorted_deduped(
                prevouts
                    .iter()
                    .map(|o| classify_script_pubkey(&o.script_pubkey_bytes()).as_u32()),
            );
            f.extend(input_types);

            // mixed_input_types
            f.push(mixed_input_types(&prevouts) as u32);

            // intra address_reuse
            f.push(address_reuse(&outputs, &prevouts) as u32);

            // input_order - sorted deduped discriminants
            let order_types = sorted_deduped(
                input_order(&inputs, &prevouts)
                    .into_iter()
                    .map(InputSortingType::as_u32),
            );
            f.extend(order_types);

            // has_uncompressed_pubkey - any input with uncompressed pubkey
            f.push(
                inputs
                    .iter()
                    .zip(prevouts.iter())
                    .any(|(inp, prevout)| has_uncompressed_pubkey(inp, prevout))
                    as u32,
            );

            // taproot_keyspend_non_default_sighash - any input with explicit sighash in taproot keyspend
            f.push(
                inputs
                    .iter()
                    .zip(prevouts.iter())
                    .any(|(inp, prevout)| taproot_keyspend_non_default_sighash(inp, prevout))
                    as u32,
            );

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
    #[allow(dead_code)]
    pub fn new(input: Expr<TxSet>) -> Expr<NormalizedFingerprints> {
        let ctx = input.context().clone();
        ctx.register(CollectFingerprintsNode::new(input))
    }
}
