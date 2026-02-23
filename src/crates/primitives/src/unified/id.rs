use crate::{dense, loose};

#[repr(transparent)]
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct AnyTxId(i32);

impl AnyTxId {
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

    pub fn raw(self) -> i32 {
        self.0
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
