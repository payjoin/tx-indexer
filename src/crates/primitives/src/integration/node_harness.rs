//! Node and wallet harness for integration tests: spawn regtest bitcoind, expose
//! blocks_dir and RPC helpers, and run the action + expected-results test harness.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use bitcoin::{Address, Amount, Block, BlockHash, Transaction, Txid};
use corepc_node::{Conf, Node};

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::confirmed::{BlockTxIndex, ConfirmedTxPtrIndex, InPrevoutIndex, OutSpentByIndex};
use crate::dense::{Parser, TxId};

/// Holds a running regtest node and its wallet RPC client. Dropping stops the node.
pub struct NodeHarness {
    _node: Node,
    pub blocks_dir: PathBuf,
}

impl NodeHarness {
    /// Start a regtest node with optional config. If `conf` is `None`, uses default
    /// (regtest, default wallet). Sets `-txindex=1` so we can query by txid.
    pub fn new(conf: Option<Conf>) -> Result<Self> {
        let mut conf = conf.unwrap_or_default();
        conf.args.push("-txindex=1");
        conf.args.push("-fallbackfee=0.00001");
        let node = Node::from_downloaded_with_conf(&conf)?;
        // Regtest stores chain data under <datadir>/regtest/; blocks are in regtest/blocks.
        let blocks_dir = node.workdir().join("regtest").join("blocks");
        Ok(Self {
            _node: node,
            blocks_dir,
        })
    }

    /// RPC client for the default wallet.
    pub fn client(&self) -> &corepc_node::Client {
        &self._node.client
    }

    /// Generate `n` blocks to `address`; returns block hashes.
    pub fn generate_blocks(
        &self,
        n: u64,
        address: &Address<bitcoin::address::NetworkChecked>,
    ) -> Result<Vec<BlockHash>> {
        let nblocks: usize = n
            .try_into()
            .map_err(|_| anyhow::anyhow!("block count too large"))?;
        let hashes = self.client().generate_to_address(nblocks, address)?;
        hashes
            .0
            .iter()
            .map(|s| {
                s.parse()
                    .map_err(|e| anyhow::anyhow!("parse block hash: {}", e))
            })
            .collect()
    }

    /// Send `amount` to `address`; returns the txid of the created tx.
    pub fn send_to_address(
        &self,
        address: &Address<bitcoin::address::NetworkChecked>,
        amount: Amount,
    ) -> Result<Txid> {
        let res = self.client().send_to_address(address, amount)?;
        Ok(res.txid()?)
    }

    /// Current block count (height + 1, 0-indexed chain length).
    pub fn get_block_count(&self) -> Result<u64> {
        let count = self.client().get_block_count()?;
        Ok(count.0)
    }

    /// Fetch block by hash.
    pub fn get_block(&self, hash: BlockHash) -> Result<Block> {
        let block = self
            .client()
            .get_block(hash)
            .map_err(|e| anyhow::anyhow!("get_block: {:?}", e))?;
        Ok(block)
    }

    /// Fetch raw transaction by txid (returns the transaction).
    pub fn get_raw_transaction(&self, txid: Txid) -> Result<Transaction> {
        let raw = self.client().get_raw_transaction(txid)?;
        Ok(raw.transaction()?)
    }

    /// Best block hash (chain tip).
    pub fn best_block_hash(&self) -> Result<BlockHash> {
        Ok(self.client().best_block_hash()?)
    }
}

/// Outcome of the test action: expected txids and block count after the action.
/// The harness uses this to know how many blocks to parse and what to pass to the expected closure.
#[derive(Clone, Debug)]
pub struct HarnessOut {
    pub expected_txids: Vec<Txid>,
    pub block_count_after: u64,
}

/// Run the full harness: create node, run action, sync, build parser, run expected.
pub fn run_harness<A, E>(action: A, expected: E) -> Result<()>
where
    A: FnOnce(&mut NodeHarness) -> Result<HarnessOut>,
    E: FnOnce(&NodeHarness, &mut Parser, &HarnessOut, &HashMap<bitcoin::Txid, TxId>) -> Result<()>,
{
    let mut harness = NodeHarness::new(None)?;
    let address = harness.client().new_address()?;
    harness.generate_blocks(101, &address)?;

    let out = action(&mut harness)?;

    // Give bitcoind time to flush block files to disk.
    std::thread::sleep(Duration::from_secs(2));

    let blocks_dir = &harness.blocks_dir;
    let blk0 = blocks_dir.join("blk00000.dat");
    if !blk0.exists() {
        let entries_len = std::fs::read_dir(blocks_dir)
            .map(|d| d.count())
            .unwrap_or(0);
        return Err(anyhow::anyhow!(
            "blk00000.dat not found at {} (blocks_dir exists: {}, entries: {})",
            blk0.display(),
            blocks_dir.exists(),
            entries_len
        ));
    }

    // block_count_after is chain height (0-based); we need to parse height+1 blocks to include the tip.
    let num_blocks = out.block_count_after + 1;
    let txptr_path = temp_txptr_path();
    let txptr_index = ConfirmedTxPtrIndex::create(&txptr_path)?;
    let block_tx_path = temp_block_tx_path();
    let block_tx_index = BlockTxIndex::create(&block_tx_path)?;
    let in_prevout_path = temp_in_prevout_path();
    let in_prevout_index = InPrevoutIndex::create(&in_prevout_path)?;
    let out_spent_path = temp_out_spent_path();
    let out_spent_index = OutSpentByIndex::create(&out_spent_path)?;
    let mut parser = Parser::new(
        harness.blocks_dir.clone(),
        txptr_index,
        block_tx_index,
        in_prevout_index,
        out_spent_index,
    );
    let txids = parser
        .parse_blocks(0..num_blocks)
        .map_err(|e| anyhow::anyhow!("parse_blocks: {:?}", e))?;

    expected(&harness, &mut parser, &out, &txids)
}

fn temp_txptr_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos();
    std::env::temp_dir().join(format!("confirmed_txptr_{}.bin", nanos))
}

fn temp_block_tx_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos();
    std::env::temp_dir().join(format!("block_tx_end_{}.bin", nanos))
}

fn temp_in_prevout_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos();
    std::env::temp_dir().join(format!("in_prevout_outid_{}.bin", nanos))
}

fn temp_out_spent_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos();
    std::env::temp_dir().join(format!("out_spent_by_inid_{}.bin", nanos))
}
