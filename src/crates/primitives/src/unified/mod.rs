use crate::dense::DenseStorage;
use crate::handle::{TxHandle, TxInHandle, TxOutHandle};
use crate::loose::InMemoryIndex;
use crate::parser::BlockFileError;
use crate::traits::graph_index::{
    IndexedGraph, OutpointIndex, PrevOutIndex, ScriptPubkeyIndex, TxInIndex, TxInOwnerIndex,
    TxIndex, TxIoIndex, TxOutDataIndex,
};
use crate::{ScriptPubkeyHash, dense, loose, traits::abstract_types::AbstractTransaction};
use crate::{dense::build_indices, loose::LooseIndexBuilder, sled::spk_db::SledScriptPubkeyDb};
use bitcoin::Amount;
use std::{ops::Range, path::PathBuf};

#[repr(transparent)]
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct AnyTxId(i32);

impl AnyTxId {
    pub fn with<'a>(self, index: &'a dyn IndexedGraph) -> TxHandle<'a> {
        TxHandle { tx_id: self, index }
    }

    pub fn is_confirmed(self) -> bool {
        self.0 >= 0
    }

    pub fn is_loose(self) -> bool {
        self.0 < 0
    }

    pub fn confirmed_txid(self) -> Option<dense::TxId> {
        if self.0 >= 0 {
            Some(dense::TxId::new(self.0 as u32))
        } else {
            None
        }
    }

    pub fn loose_txid(self) -> Option<loose::TxId> {
        if self.0 >= 0 {
            return None;
        }
        let neg = self.0.checked_neg()?;
        Some(loose::TxId::new(neg as u32))
    }
}

impl From<dense::TxId> for AnyTxId {
    fn from(txid: dense::TxId) -> Self {
        Self(i32::try_from(txid.index()).expect("dense txid should fit in i32"))
    }
}

impl From<loose::TxId> for AnyTxId {
    fn from(txid: loose::TxId) -> Self {
        let k32 = txid.index();
        assert!(k32 != 0, "loose txid must be non-zero");
        let k32 = i32::try_from(k32).expect("loose txid must fit in i32");
        Self(-k32)
    }
}

#[repr(transparent)]
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct AnyOutId(i64);

impl AnyOutId {
    pub fn with<'a>(self, index: &'a dyn IndexedGraph) -> TxOutHandle<'a> {
        TxOutHandle {
            out_id: self,
            index,
        }
    }

    pub fn is_confirmed(self) -> bool {
        self.0 >= 0
    }

    pub fn is_loose(self) -> bool {
        self.0 < 0
    }

    pub fn confirmed_id(self) -> Option<dense::TxOutId> {
        if self.0 >= 0 {
            Some(dense::TxOutId::new(self.0 as u64))
        } else {
            None
        }
    }

    pub fn loose_id(self) -> Option<loose::TxOutId> {
        if self.0 >= 0 {
            return None;
        }
        let payload = self.0.checked_neg()? as u64;
        let k32 = (payload >> 32) as u32;
        if k32 == 0 {
            return None;
        }
        let vout = payload as u32;
        Some(loose::TxOutId::new(loose::TxId::new(k32), vout))
    }

    pub fn raw(self) -> i64 {
        self.0
    }
}

impl From<dense::TxOutId> for AnyOutId {
    fn from(id: dense::TxOutId) -> Self {
        Self(i64::try_from(id.index()).expect("dense txout id should fit in i64"))
    }
}

impl From<loose::TxOutId> for AnyOutId {
    fn from(id: loose::TxOutId) -> Self {
        let k32 = id.txid().index();
        assert!(k32 != 0, "loose txid must be non-zero");
        let payload = ((k32 as u64) << 32) | (id.vout() as u64);
        let payload = i64::try_from(payload).expect("loose outid must fit in i64");
        assert!(payload != 0, "loose outid payload must be non-zero");
        Self(-payload)
    }
}

#[repr(transparent)]
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct AnyInId(i64);

impl AnyInId {
    pub fn with<'a>(self, index: &'a dyn IndexedGraph) -> TxInHandle<'a> {
        TxInHandle { in_id: self, index }
    }

    pub fn is_confirmed(self) -> bool {
        self.0 >= 0
    }

    pub fn is_loose(self) -> bool {
        self.0 < 0
    }

    pub fn confirmed_id(self) -> Option<dense::TxInId> {
        if self.0 >= 0 {
            Some(dense::TxInId::new(self.0 as u64))
        } else {
            None
        }
    }

    pub fn loose_id(self) -> Option<loose::TxInId> {
        if self.0 >= 0 {
            return None;
        }
        let payload = self.0.checked_neg()? as u64;
        let k32 = (payload >> 32) as u32;
        if k32 == 0 {
            return None;
        }
        let vin = payload as u32;
        Some(loose::TxInId::new(loose::TxId::new(k32), vin))
    }

    pub fn raw(self) -> i64 {
        self.0
    }
}

impl From<dense::TxInId> for AnyInId {
    fn from(id: dense::TxInId) -> Self {
        Self(i64::try_from(id.index()).expect("dense txin id should fit in i64"))
    }
}

impl From<loose::TxInId> for AnyInId {
    fn from(id: loose::TxInId) -> Self {
        let k32 = id.txid().index();
        assert!(k32 != 0, "loose txid must be non-zero");
        let payload = ((k32 as u64) << 32) | (id.vin() as u64);
        let payload = i64::try_from(payload).expect("loose inid must fit in i64");
        assert!(payload != 0, "loose inid payload must be non-zero");
        Self(-payload)
    }
}

pub struct DenseBuildSpec {
    pub blocks_dir: PathBuf,
    pub range: Range<u64>,
    pub paths: dense::IndexPaths,
    pub spk_db: SledScriptPubkeyDb,
}

pub struct UnifiedStorage {
    dense: Option<DenseStorage>,
    loose: Option<InMemoryIndex>,
}

impl From<LooseIndexBuilder> for UnifiedStorage {
    fn from(builder: LooseIndexBuilder) -> Self {
        Self {
            dense: None,
            loose: Some(builder.build()),
        }
    }
}

// TODO: specific error for unified storage
impl TryFrom<DenseBuildSpec> for UnifiedStorage {
    type Error = BlockFileError;

    fn try_from(spec: DenseBuildSpec) -> Result<Self, Self::Error> {
        let dense = build_indices(spec.blocks_dir, spec.range, spec.paths, spec.spk_db)?;
        Ok(Self {
            dense: Some(dense),
            loose: None,
        })
    }
}

impl UnifiedStorage {
    pub fn with_loose(mut self, builder: LooseIndexBuilder) -> Self {
        self.loose = Some(builder.build());
        self
    }

    #[inline(always)]
    fn loose(&self) -> &InMemoryIndex {
        self.loose.as_ref().expect("loose storage not initialized")
    }

    #[inline(always)]
    fn dense(&self) -> &DenseStorage {
        self.dense.as_ref().expect("dense storage not initialized")
    }

    #[inline]
    fn resolve_tx<T>(
        &self,
        id: AnyTxId,
        loose_fn: impl FnOnce(&InMemoryIndex, loose::TxId) -> T,
        dense_fn: impl FnOnce(&DenseStorage, dense::TxId) -> T,
    ) -> T {
        if let Some(lid) = id.loose_txid() {
            loose_fn(self.loose(), lid)
        } else {
            dense_fn(
                self.dense(),
                id.confirmed_txid().expect("must be dense or loose"),
            )
        }
    }

    #[inline]
    fn resolve_out<T>(
        &self,
        id: AnyOutId,
        loose_fn: impl FnOnce(&InMemoryIndex, loose::TxOutId) -> T,
        dense_fn: impl FnOnce(&DenseStorage, dense::TxOutId) -> T,
    ) -> T {
        if let Some(lid) = id.loose_id() {
            loose_fn(self.loose(), lid)
        } else {
            dense_fn(
                self.dense(),
                id.confirmed_id().expect("must be dense or loose"),
            )
        }
    }

    #[inline]
    fn resolve_in<T>(
        &self,
        id: AnyInId,
        loose_fn: impl FnOnce(&InMemoryIndex, loose::TxInId) -> T,
        dense_fn: impl FnOnce(&DenseStorage, dense::TxInId) -> T,
    ) -> T {
        if let Some(lid) = id.loose_id() {
            loose_fn(self.loose(), lid)
        } else {
            dense_fn(
                self.dense(),
                id.confirmed_id().expect("must be dense or loose"),
            )
        }
    }

    #[inline]
    fn loose_tx(&self, id: loose::TxId) -> &std::sync::Arc<dyn AbstractTransaction + Send + Sync> {
        self.loose()
            .txs
            .get(&id)
            .expect("loose txid not found in storage")
    }

    pub fn loose_txids(&self) -> Vec<AnyTxId> {
        let loose = self
            .loose
            .as_ref()
            .expect("loose storage missing when requesting loose txids");
        loose.tx_order.iter().copied().map(AnyTxId::from).collect()
    }

    pub fn loose_txids_len(&self) -> usize {
        self.loose.as_ref().map_or(0, |loose| loose.tx_order.len())
    }

    pub fn dense_txids_len(&self) -> usize {
        self.dense.as_ref().map_or(0, |dense| {
            usize::try_from(dense.tx_count()).expect("dense tx count should fit in usize")
        })
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

    pub fn dense_txids_from(&self, start: usize) -> Vec<AnyTxId> {
        let Some(dense) = self.dense.as_ref() else {
            return Vec::new();
        };
        let total = usize::try_from(dense.tx_count()).expect("dense tx count should fit in usize");
        if start >= total {
            return Vec::new();
        }
        (start..total)
            .map(|idx| dense::TxId::new(u32::try_from(idx).expect("dense txid should fit in u32")))
            .map(AnyTxId::from)
            .collect()
    }

    pub fn tx_out_ids(&self, txid: AnyTxId) -> Vec<AnyOutId> {
        self.resolve_tx(
            txid,
            |ls, lid| {
                let output_len = ls.txs[&lid].output_len();
                (0..output_len)
                    .map(|vout| AnyOutId::from(loose::TxOutId::new(lid, vout as u32)))
                    .collect()
            },
            |ds, did| {
                ds.get_txout_ids(did)
                    .into_iter()
                    .map(AnyOutId::from)
                    .collect()
            },
        )
    }

    pub fn txid_for_out(&self, out_id: AnyOutId) -> AnyTxId {
        self.resolve_out(
            out_id,
            |_, lid| AnyTxId::from(lid.txid()),
            |ds, did| AnyTxId::from(ds.txid_for_out(did)),
        )
    }

    pub fn txid_for_in(&self, in_id: AnyInId) -> AnyTxId {
        self.resolve_in(
            in_id,
            |_, lid| AnyTxId::from(lid.txid()),
            |ds, did| AnyTxId::from(ds.txid_for_in(did)),
        )
    }

    pub fn spender_for_out(&self, out_id: AnyOutId) -> Option<AnyInId> {
        self.resolve_out(
            out_id,
            |ls, lid| ls.spending_txins.get(&lid).copied().map(AnyInId::from),
            |ds, did| ds.spender_for_out(did).map(AnyInId::from),
        )
    }

    pub fn tx(&self, txid: AnyTxId) -> std::sync::Arc<dyn AbstractTransaction> {
        if let Some(loose_txid) = txid.loose_txid() {
            return self.loose_tx(loose_txid).clone();
        }
        // TODO: support confirmed tx access
        panic!("confirmed tx access not supported yet");
    }

    pub fn script_pubkey_to_txout_id(&self, script_pubkey: &ScriptPubkeyHash) -> Option<AnyOutId> {
        if let Some(ls) = self.loose.as_ref()
            && let Some(id) = ls.spk_to_txout_ids.get(script_pubkey).copied()
        {
            return Some(AnyOutId::from(id));
        }
        self.dense
            .as_ref()?
            .script_pubkey_to_txout_id(script_pubkey)
            .unwrap_or(None)
            .map(AnyOutId::from)
    }
}

impl PrevOutIndex for UnifiedStorage {
    fn prev_txout(&self, id: &AnyInId) -> Option<AnyOutId> {
        self.resolve_in(
            *id,
            |ls, lid| ls.prev_txouts.get(&lid).copied().map(AnyOutId::from),
            |ds, did| ds.prevout_for_in(did).map(AnyOutId::from),
        )
    }
}

impl TxInIndex for UnifiedStorage {
    fn spending_txin(&self, tx: &AnyOutId) -> Option<AnyInId> {
        self.spender_for_out(*tx)
    }
}

impl TxInOwnerIndex for UnifiedStorage {
    fn txid_for_in(&self, in_id: &AnyInId) -> AnyTxId {
        UnifiedStorage::txid_for_in(self, *in_id)
    }
}

impl ScriptPubkeyIndex for UnifiedStorage {
    fn script_pubkey_to_txout_id(&self, script_pubkey: &ScriptPubkeyHash) -> Option<AnyOutId> {
        UnifiedStorage::script_pubkey_to_txout_id(self, script_pubkey)
    }
}

impl TxIndex for UnifiedStorage {
    fn tx(&self, txid: &AnyTxId) -> Option<std::sync::Arc<dyn AbstractTransaction + Send + Sync>> {
        let loose_txid = txid.loose_txid()?;
        self.loose.as_ref()?.txs.get(&loose_txid).cloned()
    }
}

impl TxIoIndex for UnifiedStorage {
    fn tx_in_ids(&self, txid: &AnyTxId) -> Vec<AnyInId> {
        self.resolve_tx(
            *txid,
            |ls, lid| {
                let input_len = ls.txs[&lid].input_len();
                (0..input_len)
                    .map(|vin| AnyInId::from(loose::TxInId::new(lid, vin as u32)))
                    .collect()
            },
            |ds, did| {
                ds.get_txin_ids(did)
                    .into_iter()
                    .map(AnyInId::from)
                    .collect()
            },
        )
    }

    fn tx_out_ids(&self, txid: &AnyTxId) -> Vec<AnyOutId> {
        UnifiedStorage::tx_out_ids(self, *txid)
    }

    fn locktime(&self, txid: &AnyTxId) -> u32 {
        self.resolve_tx(
            *txid,
            |ls, lid| ls.txs[&lid].locktime(),
            |ds, did| ds.get_tx(did).lock_time.to_consensus_u32(),
        )
    }

    fn input_sequence(&self, in_id: &AnyInId) -> u32 {
        if in_id.is_loose() {
            // TODO: loose transactions don't carry sequence data in the abstract model yet.
            panic!("input_sequence not supported for loose transactions");
        }
        let did = in_id.confirmed_id().expect("must be dense");
        let ds = self.dense();
        let txid = ds.txid_for_in(did);
        let (start, _) = ds.tx_in_range(txid);
        let vin = (did.index() - start) as usize;
        ds.get_tx(txid).input[vin].sequence.0
    }

    fn witness_items(&self, in_id: &AnyInId) -> Vec<Vec<u8>> {
        if in_id.is_loose() {
            // TODO: loose transactions don't carry witness data in the abstract model yet.
            panic!("witness_items not supported for loose transactions");
        }
        let did = in_id.confirmed_id().expect("must be dense");
        let ds = self.dense();
        let txid = ds.txid_for_in(did);
        let (start, _) = ds.tx_in_range(txid);
        let vin = (did.index() - start) as usize;
        let tx = ds.get_tx(txid);
        tx.input[vin]
            .witness
            .iter()
            .map(|item| item.to_vec())
            .collect()
    }

    fn script_sig_bytes(&self, in_id: &AnyInId) -> Vec<u8> {
        if in_id.is_loose() {
            // TODO: loose transactions don't carry script sig data in the abstract model yet.
            panic!("script_sig_bytes not supported for loose transactions");
        }
        let did = in_id.confirmed_id().expect("must be dense");
        let ds = self.dense();
        let txid = ds.txid_for_in(did);
        let (start, _) = ds.tx_in_range(txid);
        let vin = (did.index() - start) as usize;
        ds.get_tx(txid).input[vin].script_sig.to_bytes()
    }

    fn block_height(&self, txid: &AnyTxId) -> Option<u64> {
        txid.confirmed_txid()
            .map(|did| self.dense().block_of_tx(did))
    }
}

impl OutpointIndex for UnifiedStorage {
    fn outpoint_for_out(&self, out_id: &AnyOutId) -> (AnyTxId, u32) {
        self.resolve_out(
            *out_id,
            |_, lid| (AnyTxId::from(lid.txid()), lid.vout()),
            |ds, did| {
                let txid = ds.txid_for_out(did);
                let (start, _) = ds.tx_out_range(txid);
                let vout = u32::try_from(did.index() - start).expect("vout should fit in u32");
                (AnyTxId::from(txid), vout)
            },
        )
    }
}

impl TxOutDataIndex for UnifiedStorage {
    fn value(&self, out_id: &AnyOutId) -> Amount {
        self.resolve_out(
            *out_id,
            |ls, lid| {
                ls.txs[&lid.txid()]
                    .output_at(lid.vout() as usize)
                    .expect("txout should be present if index is built correctly")
                    .value()
            },
            |ds, did| ds.get_txout(did).value,
        )
    }

    fn script_pubkey_hash(&self, out_id: &AnyOutId) -> ScriptPubkeyHash {
        self.resolve_out(
            *out_id,
            |ls, lid| {
                ls.txs[&lid.txid()]
                    .output_at(lid.vout() as usize)
                    .expect("txout should be present if index is built correctly")
                    .script_pubkey_hash()
            },
            |ds, did| script_pubkey_hash(&ds.get_txout(did).script_pubkey),
        )
    }

    fn script_pubkey_bytes(&self, out_id: &AnyOutId) -> Vec<u8> {
        self.resolve_out(
            *out_id,
            |ls, lid| {
                ls.txs[&lid.txid()]
                    .output_at(lid.vout() as usize)
                    .expect("txout should be present if index is built correctly")
                    .script_pubkey_bytes()
            },
            |ds, did| ds.get_txout(did).script_pubkey.to_bytes(),
        )
    }
}

impl IndexedGraph for UnifiedStorage {}

fn script_pubkey_hash(script_pubkey: &bitcoin::ScriptBuf) -> ScriptPubkeyHash {
    use bitcoin::hashes::Hash as _;
    use bitcoin::hashes::hash160::Hash as Hash160;

    Hash160::hash(script_pubkey.as_bytes()).to_byte_array()
}

#[cfg(test)]
mod tests {
    use super::AnyTxId;
    use crate::{
        dense::{TxId as DenseTxId, TxInId as DenseTxInId, TxOutId as DenseTxOutId},
        loose::{TxId as LooseTxId, TxInId as LooseTxInId, TxOutId as LooseTxOutId},
    };

    #[test]
    fn any_txid_round_trip_confirmed() {
        let txid = DenseTxId::new(42);
        let any: AnyTxId = txid.into();
        assert!(any.is_confirmed());
        assert_eq!(any.confirmed_txid(), Some(txid));
        assert_eq!(any.loose_txid(), None);
    }

    #[test]
    fn any_txid_round_trip_loose() {
        let any: AnyTxId = LooseTxId::new(7).into();
        assert!(any.is_loose());
        assert_eq!(any.loose_txid(), Some(LooseTxId::new(7)));
        assert_eq!(any.confirmed_txid(), None);
    }

    #[test]
    fn any_txid_rejects_zero_loose_key() {
        let result = std::panic::catch_unwind(|| AnyTxId::from(LooseTxId::new(0)));
        assert!(result.is_err());
    }

    #[test]
    fn any_out_id_round_trip_confirmed() {
        let id = DenseTxOutId::new(42);
        let any: super::AnyOutId = id.into();
        assert!(any.is_confirmed());
        assert_eq!(any.confirmed_id(), Some(id));
        assert_eq!(any.loose_id(), None);
    }

    #[test]
    fn any_out_id_round_trip_loose() {
        let id = LooseTxOutId::new(LooseTxId::new(7), 3);
        let any: super::AnyOutId = id.into();
        assert!(any.is_loose());
        assert_eq!(any.loose_id(), Some(id));
        assert_eq!(any.confirmed_id(), None);
    }

    #[test]
    fn any_out_id_rejects_zero_loose_key() {
        let id = LooseTxOutId::new(LooseTxId::new(0), 1);
        let result = std::panic::catch_unwind(|| super::AnyOutId::from(id));
        assert!(result.is_err());
    }

    #[test]
    fn any_in_id_round_trip_confirmed() {
        let id = DenseTxInId::new(99);
        let any: super::AnyInId = id.into();
        assert!(any.is_confirmed());
        assert_eq!(any.confirmed_id(), Some(id));
        assert_eq!(any.loose_id(), None);
    }

    #[test]
    fn any_in_id_round_trip_loose() {
        let id = LooseTxInId::new(LooseTxId::new(11), 4);
        let any: super::AnyInId = id.into();
        assert!(any.is_loose());
        assert_eq!(any.loose_id(), Some(id));
        assert_eq!(any.confirmed_id(), None);
    }

    #[test]
    fn any_in_id_rejects_zero_loose_key() {
        let id = LooseTxInId::new(LooseTxId::new(0), 2);
        let result = std::panic::catch_unwind(|| super::AnyInId::from(id));
        assert!(result.is_err());
    }
}
