use bitcoin::Amount;
use bitcoin::hashes::Hash as _;
use bitcoin::hashes::hash160::Hash as Hash160;

use crate::{
    AnyOutId, AnyTxId, OutputType, ScriptPubkeyHash,
    loose::{TxId, TxOutId},
    traits::abstract_types::{
        AbstractTransaction, AbstractTxIn, AbstractTxOut, EnumerateOutputValueInArbitraryOrder,
        EnumerateSpentTxOuts, HasScriptPubkey, InputCount, OutputCount, TxConstituent,
    },
    traits::{HasBlockHeight, HasInputPrevOuts, HasNLockTime, HasSequence},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DummyTxData {
    outputs: Vec<DummyTxOutData>,
    /// Inputs of this transaction; each carries its prev outpoint and sequence.
    inputs: Vec<DummyTxInWrapper>,
    n_locktime: u32,
}

pub const SEQUENCE_FINAL: u32 = u32::MAX;

impl DummyTxData {
    /// Base constructor.
    pub fn new(
        outputs: Vec<DummyTxOutData>,
        spent_coins: Vec<TxOutId>,
        sequences: Vec<u32>,
        n_locktime: u32,
    ) -> Self {
        assert_eq!(
            spent_coins.len(),
            sequences.len(),
            "spent_coins and sequences must have matching lengths"
        );
        let inputs = spent_coins
            .into_iter()
            .zip(sequences)
            .map(|(coin, sequence)| DummyTxInWrapper {
                prev_txid: coin.txid(),
                prev_vout: coin.vout(),
                sequence,
            })
            .collect();
        Self {
            outputs,
            inputs,
            n_locktime,
        }
    }

    /// Tx with explicit outputs, no spent coins.
    pub fn new_with_outputs(outputs: Vec<DummyTxOutData>) -> Self {
        Self {
            outputs,
            inputs: vec![],
            n_locktime: 0,
        }
    }

    /// Create funding tx from amounts.
    pub fn new_with_amounts(amounts: Vec<u64>) -> Self {
        let outputs = amounts
            .into_iter()
            .enumerate()
            .map(|(vout, amount)| DummyTxOutData::new(amount, vout as u32))
            .collect();
        Self::new_with_outputs(outputs)
    }

    /// Create spending tx from amounts and spent coins.
    pub fn new_with_spent(amounts: Vec<u64>, spent_coins: Vec<TxOutId>) -> Self {
        let base = Self::new_with_amounts(amounts);
        let n = spent_coins.len();
        Self::new(base.outputs, spent_coins, vec![SEQUENCE_FINAL; n], 0)
    }

    pub fn spent_coins(&self) -> Vec<TxOutId> {
        self.inputs
            .iter()
            .map(|i| TxOutId::new(i.prev_txid, i.prev_vout))
            .collect()
    }
}

impl HasNLockTime for DummyTxData {
    fn n_locktime(&self) -> u32 {
        self.n_locktime
    }
}

impl HasBlockHeight for DummyTxData {
    fn block_height(&self) -> Option<u64> {
        None
    }
}

impl HasInputPrevOuts for DummyTxData {
    fn input_prev_types(&self) -> impl Iterator<Item = Option<OutputType>> {
        std::iter::repeat_n(None, self.inputs.len())
    }
    fn input_prev_script_hashes(&self) -> impl Iterator<Item = Option<ScriptPubkeyHash>> {
        std::iter::repeat_n(None, self.inputs.len())
    }
}

// Wrapper types for implementing abstract traits on dummy types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DummyTxInWrapper {
    prev_txid: TxId,
    prev_vout: u32,
    sequence: u32,
}

impl HasSequence for DummyTxInWrapper {
    fn sequence(&self) -> u32 {
        self.sequence
    }
}

impl AbstractTxIn for DummyTxInWrapper {
    fn prev_txid(&self) -> Option<AnyTxId> {
        Some(AnyTxId::from(self.prev_txid))
    }

    fn prev_vout(&self) -> Option<u32> {
        Some(self.prev_vout)
    }

    fn prev_txout_id(&self) -> Option<AnyOutId> {
        Some(AnyOutId::from(TxOutId::new(self.prev_txid, self.prev_vout)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DummyTxOutData {
    value: u64,
    vout: u32,
    script_pubkey: Vec<u8>,
}

impl DummyTxOutData {
    /// Create a new output with empty script pubkey.
    pub fn new(amount: u64, vout: u32) -> Self {
        Self {
            value: amount,
            vout,
            script_pubkey: vec![],
        }
    }

    /// Create a new output with explicit script pubkey.
    pub fn new_with_script(amount: u64, vout: u32, script_pubkey: impl Into<Vec<u8>>) -> Self {
        Self {
            value: amount,
            vout,
            script_pubkey: script_pubkey.into(),
        }
    }
}

impl HasScriptPubkey for DummyTxOutData {
    fn script_pubkey_bytes(&self) -> Vec<u8> {
        self.script_pubkey.clone()
    }
}

impl AbstractTxOut for DummyTxOutData {
    fn value(&self) -> Amount {
        Amount::from_sat(self.value)
    }

    fn script_pubkey_hash(&self) -> ScriptPubkeyHash {
        Hash160::hash(&self.script_pubkey).to_byte_array()
    }
}

impl AbstractTransaction for DummyTxData {
    fn inputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxIn + '_>> + '_> {
        // Collect into a vector to avoid lifetime issues
        let inputs: Vec<Box<dyn AbstractTxIn>> = self
            .inputs
            .iter()
            .map(|input| Box::new(input.clone()) as Box<dyn AbstractTxIn>)
            .collect();
        Box::new(inputs.into_iter())
    }

    fn outputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxOut + '_>> + '_> {
        // Collect into a vector to avoid lifetime issues
        let outputs: Vec<Box<dyn AbstractTxOut>> = self
            .outputs
            .iter()
            .map(|output| Box::new(output.clone()) as Box<dyn AbstractTxOut>)
            .collect();
        Box::new(outputs.into_iter())
    }

    fn input_len(&self) -> usize {
        self.inputs.len()
    }

    fn output_len(&self) -> usize {
        self.outputs.len()
    }

    fn output_at(&self, index: usize) -> Option<Box<dyn AbstractTxOut + '_>> {
        self.outputs
            .get(index)
            .map(|output| Box::new(output.clone()) as Box<dyn AbstractTxOut>)
    }

    fn locktime(&self) -> u32 {
        self.n_locktime
    }

    fn is_coinbase(&self) -> bool {
        self.inputs.is_empty()
    }
}

impl OutputCount for DummyTxData {
    fn output_count(&self) -> usize {
        self.outputs.len()
    }
}

impl InputCount for DummyTxData {
    fn input_count(&self) -> usize {
        self.inputs.len()
    }
}

impl EnumerateOutputValueInArbitraryOrder for DummyTxData {
    fn output_values(&self) -> impl Iterator<Item = Amount> {
        self.outputs.iter().map(|output| output.value())
    }
}

pub fn temp_dir(prefix: &str) -> std::path::PathBuf {
    use std::time::{SystemTime, UNIX_EPOCH};

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("{}_{}", prefix, nanos));
    std::fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

pub fn write_single_block_file(dir: &std::path::Path, block: &[u8]) -> std::io::Result<()> {
    use std::fs::File;
    use std::io::Write;

    let path = dir.join("blk00000.dat");
    let mut file = File::create(path)?;
    file.write_all(&[0xF9, 0xBE, 0xB4, 0xD9])?;
    let size = u32::try_from(block.len()).expect("block too large for u32");
    file.write_all(&size.to_le_bytes())?;
    file.write_all(block)?;
    Ok(())
}

impl EnumerateSpentTxOuts for DummyTxData {
    fn spent_coins(&self) -> impl Iterator<Item = AnyOutId> {
        self.inputs
            .iter()
            .map(|i| AnyOutId::from(TxOutId::new(i.prev_txid, i.prev_vout)))
    }
}

impl From<DummyTxData> for Box<dyn AbstractTransaction + Send + Sync> {
    fn from(val: DummyTxData) -> Self {
        Box::new(val)
    }
}

pub struct DummyTxOut {
    pub vout: usize,
    pub containing_tx: DummyTxData,
}

impl TxConstituent for DummyTxOut {
    type Handle = DummyTxData;
    fn containing_tx(&self) -> Self::Handle {
        self.containing_tx.clone()
    }

    fn vout(&self) -> usize {
        self.vout
    }
}
