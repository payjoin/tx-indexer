use crate::{
    wallet::{AddressHandle, AddressId, WalletHandle, WalletHandleMut},
    Simulation,
};
use bitcoin::{
    consensus::Decodable,
    hashes::Hash,
    transaction::{predict_weight, InputWeightPrediction},
    Amount, FeeRate, ScriptBuf, Weight, WitnessProgram,
};
use tx_indexer_primitives::{
    loose::{TxId as LooseTxId, TxOutId as LooseTxOutId},
    traits::abstract_types::{
        AbstractTransaction, AbstractTxIn, AbstractTxOut, EnumerateOutputValueInArbitraryOrder,
        EnumerateSpentTxOuts, HasNLockTime, HasPrevOutpoint, HasScriptPubkey, HasScriptSig,
        HasSequence, HasValue, HasVersion, HasWitness, OutputCount,
    },
    AnyOutId,
};

define_entity!(
    Tx,
    {
        // version, locktime, witness flag
        pub(crate) inputs: Vec<Input>,
        pub(crate) outputs: Vec<Output>,
    },
    {
        pub(crate) fee: Amount,
        pub(crate) weight: Weight,
    }
);

impl From<TxId> for bitcoin::Txid {
    fn from(txid: TxId) -> Self {
        let mut buf = [0u8; 32];
        let txid_bytes = txid.0.to_le_bytes();
        buf[..txid_bytes.len()].copy_from_slice(&txid_bytes);
        bitcoin::Txid::consensus_decode(&mut &buf[..]).expect("32 bytes should never fail")
    }
}

impl From<bitcoin::Txid> for TxId {
    fn from(bitcoin_txid: bitcoin::Txid) -> Self {
        let bytes = bitcoin_txid.as_byte_array();
        // Extract first 8 bytes and convert to usize (little endian)
        let mut txid_bytes = [0u8; 8];
        txid_bytes.copy_from_slice(&bytes[..8]);
        TxId(u64::from_le_bytes(txid_bytes) as usize)
    }
}

fn to_loose_txid(txid: TxId) -> LooseTxId {
    LooseTxId::new(u32::try_from(txid.0).expect("txid should fit in u32"))
}

// TODO rename to OutputId?
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub(crate) struct Outpoint {
    pub(crate) txid: TxId,
    pub(crate) index: usize,
}

impl<'a> Outpoint {
    pub(crate) fn with(&self, sim: &'a Simulation) -> OutputHandle<'a> {
        OutputHandle {
            sim,
            outpoint: *self,
        }
    }
}

impl From<Outpoint> for bitcoin::OutPoint {
    fn from(outpoint: Outpoint) -> Self {
        bitcoin::OutPoint::new(outpoint.txid.into(), outpoint.index as u32)
    }
}

// TODO rename to InputData?
#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub(crate) struct Input {
    pub(crate) outpoint: Outpoint, // sequence,
                                   // witness?
}

impl HasPrevOutpoint for Input {
    fn prev_outpoint_txid_bytes(&self) -> [u8; 32] {
        let bitcoin_txid: bitcoin::Txid = self.outpoint.txid.into();
        *bitcoin_txid.as_byte_array()
    }

    fn prev_outpoint_vout(&self) -> u32 {
        u32::try_from(self.outpoint.index).expect("vout index should fit in u32")
    }
}

impl HasSequence for Input {
    fn sequence(&self) -> u32 {
        0
    }
}

impl HasWitness for Input {
    fn witness_items(&self) -> Vec<Vec<u8>> {
        vec![]
    }
}

impl HasScriptSig for Input {
    fn script_sig_bytes(&self) -> Vec<u8> {
        vec![]
    }
}

impl AbstractTxIn for Input {}

#[derive(Debug, PartialEq, Clone, Copy, Eq, PartialOrd, Ord)]
pub(crate) struct InputId {
    pub(crate) txid: TxId,
    pub(crate) index: usize,
}

impl From<InputId> for Outpoint {
    fn from(id: InputId) -> Self {
        Outpoint {
            txid: id.txid,
            index: id.index,
        }
    }
}

pub(crate) struct InputHandle<'a> {
    sim: &'a Simulation,
    pub(crate) id: InputId,
}

impl<'a> InputHandle<'a> {
    pub(crate) fn data(&self) -> &'a Input {
        &self.id.txid.with(self.sim).data().inputs[self.id.index]
    }

    pub(crate) fn prevout(&self) -> OutputHandle<'a> {
        self.data().outpoint.with(self.sim)
    }
}

impl<'a> From<InputHandle<'a>> for Output {
    fn from(handle: InputHandle<'a>) -> Output {
        *handle.prevout().data()
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) struct Output {
    pub(crate) amount: Amount,
    pub(crate) address_id: AddressId,
}

impl HasValue for Output {
    fn value(&self) -> Amount {
        self.amount
    }
}

impl HasScriptPubkey for Output {
    fn script_pubkey_bytes(&self) -> Vec<u8> {
        unimplemented!("fungi outputs do not track script pubkey bytes")
    }
}

impl AbstractTxOut for Output {}

impl From<Output> for bitcoin::transaction::TxOut {
    fn from(o: Output) -> Self {
        // FIXME refactor into fn encode_as_txo(enum { AddressId, Index, Outpoint  })
        // TODO handle multiple address types
        let mut program = [0u8; 32];
        // TODO tag, segregate from txos encoding indexes?
        program[0] = o
            .address_id
            .0
            .try_into()
            .expect("TODO support more than 256 addresses");

        let witness_program =
            WitnessProgram::new(bitcoin::WitnessVersion::V1, &program[..]).unwrap();
        let script_pubkey = ScriptBuf::new_witness_program(&witness_program);

        bitcoin::transaction::TxOut {
            value: o.amount,
            script_pubkey,
        }
    }
}

impl Output {
    #[allow(dead_code)]
    fn script_pubkey_len(&self, sim: &Simulation) -> usize {
        self.address(sim).data().script_type.output_script_len()
    }

    fn address<'a>(&self, sim: &'a Simulation) -> AddressHandle<'a> {
        self.address_id.with(sim)
    }

    fn wallet<'a>(&self, sim: &'a Simulation) -> WalletHandle<'a> {
        self.address(sim).wallet()
    }

    #[allow(dead_code)]
    fn wallet_mut<'a>(&self, sim: &'a mut Simulation) -> WalletHandleMut<'a> {
        let owner_id = self.address(sim).data().wallet_id;
        owner_id.with_mut(sim)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct OutputHandle<'a> {
    sim: &'a Simulation,
    pub(crate) outpoint: Outpoint,
}

impl From<OutputHandle<'_>> for InputWeightPrediction {
    fn from(output: OutputHandle<'_>) -> Self {
        Self::from(output.address())
    }
}

impl<'a> OutputHandle<'a> {
    pub(crate) fn new(sim: &'a Simulation, outpoint: Outpoint) -> Self {
        Self { sim, outpoint }
    }

    pub(crate) fn outpoint(&self) -> Outpoint {
        self.outpoint
    }

    pub(crate) fn data(&self) -> &'a Output {
        &self.outpoint.txid.with(self.sim).data().outputs[self.outpoint.index]
    }

    pub(crate) fn address(&'a self) -> AddressHandle<'a> {
        self.data().address_id.with(self.sim)
    }

    pub(crate) fn wallet(&'a self) -> WalletHandle<'a> {
        self.data().wallet(self.sim)
    }

    #[allow(dead_code)]
    pub(crate) fn wallet_mut<'b>(&self, sim: &'b mut Simulation) -> WalletHandleMut<'b> {
        self.data().wallet_mut(sim)
    }
}

impl<'a> From<OutputHandle<'a>> for Output {
    fn from(handle: OutputHandle<'a>) -> Output {
        *handle.data()
    }
}

impl<'a> From<OutputHandle<'a>> for Outpoint {
    fn from(handle: OutputHandle<'a>) -> Outpoint {
        handle.outpoint
    }
}

impl<'a> TxHandle<'a> {
    pub(crate) fn data(&self) -> &'a TxData {
        &self.sim.tx_data[self.id.0]
    }

    pub(crate) fn info(&self) -> &'a TxInfo {
        &self.sim.tx_info[self.id.0]
    }

    pub(crate) fn is_coinbase(&self) -> bool {
        self.data().inputs.is_empty()
    }

    pub(crate) fn outpoints(&self) -> impl Iterator<Item = Outpoint> {
        let txid = self.id;
        (0..self.data().outputs.len()).map(move |index| Outpoint { txid, index })
    }
    pub(crate) fn outputs(&'a self) -> impl Iterator<Item = OutputHandle<'a>> {
        self.outpoints().map(|outpoint| OutputHandle {
            sim: self.sim,
            outpoint,
        })
    }

    pub(crate) fn inputs(&'a self) -> impl Iterator<Item = InputHandle<'a>> {
        let txid = self.id;
        let sim = self.sim;
        (0..self.data().inputs.len()).map(move |index| InputHandle {
            sim,
            id: InputId { txid, index },
        })
    }

    #[allow(dead_code)]
    pub(crate) fn is_confirmed(&self) -> bool {
        self.sim
            .block_data
            .iter()
            .any(|block| block.confirmed_txs.contains(&self.id))
    }

    // TODO fn prevouts(self) -> impl IntoIterator??
    // TODO previous txs
}

#[allow(clippy::derivable_impls)]
#[allow(dead_code)]
impl Default for TxData {
    fn default() -> Self {
        Self {
            inputs: Vec::default(),
            outputs: Vec::default(),
        }
    }
}

impl AbstractTransaction for TxData {
    fn inputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxIn + '_>> + '_> {
        let inputs: Vec<Box<dyn AbstractTxIn>> = self
            .inputs
            .iter()
            .copied()
            .map(|input| Box::new(input) as Box<dyn AbstractTxIn>)
            .collect();
        Box::new(inputs.into_iter())
    }

    fn outputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxOut + '_>> + '_> {
        let outputs: Vec<Box<dyn AbstractTxOut>> = self
            .outputs
            .iter()
            .copied()
            .map(|output| Box::new(output) as Box<dyn AbstractTxOut>)
            .collect();
        Box::new(outputs.into_iter())
    }

    fn output_len(&self) -> usize {
        self.outputs.len()
    }

    fn output_at(&self, index: usize) -> Option<Box<dyn AbstractTxOut + '_>> {
        self.outputs
            .get(index)
            .copied()
            .map(|output| Box::new(output) as Box<dyn AbstractTxOut>)
    }

    fn input_len(&self) -> usize {
        self.inputs.len()
    }

    fn is_coinbase(&self) -> bool {
        self.inputs.is_empty()
    }
}

impl OutputCount for TxData {
    fn output_count(&self) -> usize {
        self.outputs.len()
    }
}

impl EnumerateOutputValueInArbitraryOrder for TxData {
    fn output_values(&self) -> impl Iterator<Item = Amount> {
        self.outputs.iter().map(|output| output.amount)
    }
}

impl EnumerateSpentTxOuts for TxData {
    fn spent_coins(&self) -> impl Iterator<Item = AnyOutId> {
        self.inputs.iter().map(|input| {
            AnyOutId::from(LooseTxOutId::new(
                to_loose_txid(input.outpoint.txid),
                u32::try_from(input.outpoint.index).expect("vout index should fit in u32"),
            ))
        })
    }
}

impl HasNLockTime for TxData {
    fn locktime(&self) -> u32 {
        unimplemented!("fungi tx data does not track locktime")
    }
}

impl HasVersion for TxData {
    fn version(&self) -> i32 {
        unimplemented!("fungi tx data does not track version")
    }
}

impl TxInfo {
    pub(crate) fn new(tx: &TxData, sim: &Simulation) -> Self {
        // TODO Result with invalid txn error?
        // TODO refactor into a method.. on Simulation? on tx accepting simulation?
        let prevouts = tx.inputs.iter().map(|i| i.outpoint.with(sim));

        let weight = predict_weight(
            prevouts.clone().map(InputWeightPrediction::from),
            tx.outputs.iter().map(|o| o.script_pubkey_len(sim)),
        );

        // TODO separate to a different index struct
        let total_input_amount: Amount = prevouts.map(|o| o.data().amount).sum();
        let total_output_amount = tx.outputs.iter().map(|o| o.amount).sum();

        // TODO Result
        assert!(tx.inputs.is_empty() || total_output_amount <= total_input_amount);

        let fees = if tx.inputs.is_empty() {
            Amount::default()
        } else {
            total_input_amount - total_output_amount // TODO
        };

        TxInfo { fee: fees, weight }
    }

    pub(crate) fn feerate(self) -> FeeRate {
        self.fee / self.weight
    }
}

#[cfg(test)]
mod tests {
    use bitcoin::hashes::Hash;

    use super::*;

    #[test]
    fn test_txid_encoding() {
        let txid = TxId(1);
        let bitcoin_txid = bitcoin::Txid::from(txid);

        assert_eq!(bitcoin_txid.as_byte_array()[0], 1);
        assert_eq!(bitcoin_txid.to_byte_array()[1..], [0u8; 31]);

        let converted_back = TxId::from(bitcoin_txid);
        assert_eq!(converted_back, txid);
    }
}
