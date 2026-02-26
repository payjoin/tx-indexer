use bitcoin::Amount;

use crate::{
    AnyOutId, AnyTxId, ScriptPubkeyHash,
    loose::{TxId, TxOutId},
    traits::HasNLockTime,
    traits::abstract_types::{
        AbstractTransaction, AbstractTxIn, AbstractTxOut, EnumerateOutputValueInArbitraryOrder,
        EnumerateSpentTxOuts, OutputCount, TxConstituent,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DummyTxData {
    pub id: TxId,
    pub outputs: Vec<DummyTxOutData>,
    /// The outputs that are spent by this transaction
    pub spent_coins: Vec<TxOutId>,
    pub n_locktime: u32,
}

impl DummyTxData {
    pub fn new(
        id: TxId,
        outputs: Vec<DummyTxOutData>,
        spent_coins: Vec<TxOutId>,
        n_locktime: u32,
    ) -> Self {
        Self {
            id,
            outputs,
            spent_coins,
            n_locktime,
        }
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
    spk_hash: ScriptPubkeyHash,
    vout: u32,
    containing_txid: TxId,
}

impl DummyTxOutData {
    pub fn new(value: u64, spk_hash: ScriptPubkeyHash, vout: u32, containing_txid: TxId) -> Self {
        Self {
            value,
            spk_hash,
            vout,
            containing_txid,
        }
    }

    /// Create a new DummyTxOutData with a given amount and a dummy spk hash
    pub fn new_with_amount(amount: u64, vout: u32, containing_txid: TxId) -> Self {
        Self {
            value: amount,
            spk_hash: [0u8; 20],
            vout,
            containing_txid,
        }
    }
}

impl AbstractTxOut for DummyTxOutData {
    fn id(&self) -> AnyOutId {
        AnyOutId::from(TxOutId::new(self.containing_txid, self.vout))
    }

    fn value(&self) -> Amount {
        Amount::from_sat(self.value)
    }

    fn script_pubkey_hash(&self) -> ScriptPubkeyHash {
        self.spk_hash
    }
}

impl AbstractTransaction for DummyTxData {
    fn id(&self) -> AnyTxId {
        AnyTxId::from(self.id)
    }

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
}

impl OutputCount for DummyTxData {
    fn output_count(&self) -> usize {
        self.outputs.len()
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
