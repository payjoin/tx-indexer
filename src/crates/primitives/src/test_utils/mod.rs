use bitcoin::Amount;
use bitcoin::hashes::Hash as _;
use bitcoin::hashes::hash160::Hash as Hash160;

use crate::{
    AnyOutId, AnyTxId, ScriptPubkeyHash,
    loose::{TxId, TxOutId},
    traits::HasNLockTime,
    traits::abstract_types::{
        AbstractTransaction, AbstractTxIn, AbstractTxOut, EnumerateOutputValueInArbitraryOrder,
        EnumerateSpentTxOuts, InputCount, OutputCount, TxConstituent,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DummyTxData {
    outputs: Vec<DummyTxOutData>,
    /// The outputs that are spent by this transaction
    spent_coins: Vec<TxOutId>,
    n_locktime: u32,
}

impl DummyTxData {
    /// Base constructor.
    pub fn new(outputs: Vec<DummyTxOutData>, spent_coins: Vec<TxOutId>, n_locktime: u32) -> Self {
        Self {
            outputs,
            spent_coins,
            n_locktime,
        }
    }
    /// Tx with explicit outputs, no spent coins.
    pub fn new_with_outputs(outputs: Vec<DummyTxOutData>) -> Self {
        Self {
            outputs,
            spent_coins: vec![],
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
        Self::new(base.outputs, spent_coins, 0)
    }

    pub fn spent_coins(&self) -> &[TxOutId] {
        &self.spent_coins
    }
}

impl HasNLockTime for DummyTxData {
    fn n_locktime(&self) -> u32 {
        self.n_locktime
    }
}

// Wrapper types for implementing abstract traits on dummy types
struct DummyTxInWrapper {
    prev_txid: TxId,
    prev_vout: u32,
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

impl AbstractTxOut for DummyTxOutData {
    fn value(&self) -> Amount {
        Amount::from_sat(self.value)
    }

    fn script_pubkey_hash(&self) -> ScriptPubkeyHash {
        Hash160::hash(&self.script_pubkey).to_byte_array()
    }

    fn script_pubkey_bytes(&self) -> Vec<u8> {
        self.script_pubkey.clone()
    }
}

impl AbstractTransaction for DummyTxData {
    fn inputs(&self) -> Box<dyn Iterator<Item = Box<dyn AbstractTxIn + '_>> + '_> {
        // Collect into a vector to avoid lifetime issues
        let inputs: Vec<Box<dyn AbstractTxIn>> = self
            .spent_coins
            .iter()
            .map(|spent| {
                Box::new(DummyTxInWrapper {
                    prev_txid: spent.txid(),
                    prev_vout: spent.vout(),
                }) as Box<dyn AbstractTxIn>
            })
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
        self.spent_coins.len()
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
        self.spent_coins.is_empty()
    }
}

impl OutputCount for DummyTxData {
    fn output_count(&self) -> usize {
        self.outputs.len()
    }
}

impl InputCount for DummyTxData {
    fn input_count(&self) -> usize {
        self.spent_coins.len()
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
        self.spent_coins.iter().copied().map(AnyOutId::from)
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
