use std::{collections::HashMap, ops::Range, path::PathBuf};

use crate::{
    dense::{self, BlockFileError, DenseStorage, IndexPaths, build_indices},
    loose::{
        self,
        storage::{InMemoryIndex, LooseIndexBuilder},
    },
    unified::id::{AnyInId, AnyOutId, AnyTxId},
};

pub struct DenseBuildSpec {
    pub blocks_dir: PathBuf,
    pub range: Range<u64>,
    pub paths: IndexPaths,
}

pub struct UnifiedStorageBuilder {
    dense: Option<DenseBuildSpec>,
    loose: Option<LooseIndexBuilder>,
}

impl UnifiedStorageBuilder {
    pub fn new() -> Self {
        Self {
            dense: None,
            loose: None,
        }
    }

    pub fn with_dense(mut self, spec: DenseBuildSpec) -> Self {
        self.dense = Some(spec);
        self
    }

    pub fn with_loose(mut self, builder: LooseIndexBuilder) -> Self {
        self.loose = Some(builder);
        self
    }

    // TODO: specific error for unified storage
    pub fn build(self) -> Result<UnifiedStorageBuild, BlockFileError> {
        let (dense, dense_txids) = if let Some(spec) = self.dense {
            let (storage, txids) = build_indices(spec.blocks_dir, spec.range, spec.paths)?;
            (Some(storage), Some(txids))
        } else {
            (None, None)
        };

        let loose = self.loose.map(|builder| builder.build());

        Ok(UnifiedStorageBuild {
            storage: UnifiedStorage { dense, loose },
            dense_txids,
        })
    }
}

impl Default for UnifiedStorageBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct UnifiedStorageBuild {
    pub storage: UnifiedStorage,
    pub dense_txids: Option<HashMap<bitcoin::Txid, dense::TxId>>,
}

pub struct UnifiedStorage {
    pub dense: Option<DenseStorage>,
    pub loose: Option<InMemoryIndex>,
}

impl UnifiedStorage {
    pub fn new(dense: Option<DenseStorage>, loose: Option<InMemoryIndex>) -> Self {
        Self { dense, loose }
    }

    pub fn loose_txids(&self) -> Vec<AnyTxId> {
        let loose = self
            .loose
            .as_ref()
            .expect("loose storage missing when requesting loose txids");
        loose.tx_order.iter().copied().map(AnyTxId::from).collect()
    }

    pub fn loose_txids_len(&self) -> usize {
        let loose = self
            .loose
            .as_ref()
            .expect("loose storage missing when requesting loose txids");
        loose.tx_order.len()
    }

    pub fn loose_txids_from(&self, start: usize) -> Vec<AnyTxId> {
        let loose = self
            .loose
            .as_ref()
            .expect("loose storage missing when requesting loose txids");
        if start >= loose.tx_order.len() {
            return Vec::new();
        }
        loose.tx_order[start..]
            .iter()
            .copied()
            .map(AnyTxId::from)
            .collect()
    }

    pub fn tx_out_ids(&self, txid: AnyTxId) -> Vec<AnyOutId> {
        if let Some(loose_txid) = txid.loose_txid() {
            let loose = self
                .loose
                .as_ref()
                .expect("loose storage missing for loose txid");
            let tx = loose
                .txs
                .get(&loose_txid)
                .expect("loose txid not found in storage");
            let output_len = tx.output_len();
            return (0..output_len)
                .map(|vout| AnyOutId::from(loose::TxOutId::new(loose_txid, vout as u32)))
                .collect();
        }

        let dense_txid = txid
            .confirmed_txid()
            .expect("confirmed txid must map to dense txid");
        let dense = self
            .dense
            .as_ref()
            .expect("dense storage missing for confirmed txid");
        dense
            .get_txout_ids(dense_txid)
            .into_iter()
            .map(AnyOutId::from)
            .collect()
    }

    pub fn txid_for_out(&self, out_id: AnyOutId) -> AnyTxId {
        if let Some(loose_outid) = out_id.loose_id() {
            return AnyTxId::from(loose_outid.txid());
        }
        let dense_outid = out_id
            .confirmed_id()
            .expect("confirmed outid must map to dense outid");
        let dense = self
            .dense
            .as_ref()
            .expect("dense storage missing for confirmed outid");
        AnyTxId::from(dense.txid_for_out(dense_outid))
    }

    pub fn txid_for_in(&self, in_id: AnyInId) -> AnyTxId {
        if let Some(loose_inid) = in_id.loose_id() {
            return AnyTxId::from(loose_inid.txid());
        }
        let dense_inid = in_id
            .confirmed_id()
            .expect("confirmed inid must map to dense inid");
        let dense = self
            .dense
            .as_ref()
            .expect("dense storage missing for confirmed inid");
        AnyTxId::from(dense.txid_for_in(dense_inid))
    }

    pub fn spender_for_out(&self, out_id: AnyOutId) -> Option<AnyInId> {
        if let Some(loose_outid) = out_id.loose_id() {
            let loose = self
                .loose
                .as_ref()
                .expect("loose storage missing for loose outid");
            return loose
                .spending_txins
                .get(&loose_outid)
                .copied()
                .map(AnyInId::from);
        }
        let dense_outid = out_id
            .confirmed_id()
            .expect("confirmed outid must map to dense outid");
        let dense = self
            .dense
            .as_ref()
            .expect("dense storage missing for confirmed outid");
        dense.spender_for_out(dense_outid).map(AnyInId::from)
    }

    pub fn tx(
        &self,
        txid: AnyTxId,
    ) -> std::sync::Arc<dyn crate::abstract_types::AbstractTransaction + Send + Sync> {
        if let Some(loose_txid) = txid.loose_txid() {
            let loose = self
                .loose
                .as_ref()
                .expect("loose storage missing for loose txid");
            return loose
                .txs
                .get(&loose_txid)
                .cloned()
                .expect("loose txid not found in storage");
        }
        panic!("confirmed tx access not supported yet");
    }

    pub fn script_pubkey_to_txout_id(
        &self,
        script_pubkey: &crate::ScriptPubkeyHash,
    ) -> Option<AnyOutId> {
        let loose = self
            .loose
            .as_ref()
            .expect("loose storage missing for script pubkey index");
        loose
            .spk_to_txout_ids
            .get(script_pubkey)
            .copied()
            .map(AnyOutId::from)
    }
}
