//! Backend-agnostic abstract ID types for use in the pipeline.
//!
//! These types can hold either loose or dense concrete IDs and convert
//! to/from concrete IDs when a type-correct index is provided.

use crate::dense::{TxId as DenseTxId, TxInId as DenseTxInId, TxOutId as DenseTxOutId};
use crate::loose::{TxId as LooseTxId, TxInId as LooseTxInId, TxOutId as LooseTxOutId};

pub trait AbstractId {
    type TxId: Eq + std::hash::Hash + Copy + Send + Sync + 'static;
    type TxInId: Eq + std::hash::Hash + Copy + Send + Sync + 'static;
    type TxOutId: Eq + std::hash::Hash + Copy + Send + Sync + 'static;
}

pub struct LooseIds;

impl AbstractId for LooseIds {
    type TxId = LooseTxId;
    type TxInId = LooseTxInId;
    type TxOutId = LooseTxOutId;
}

/// Abstract transaction ID that can represent any backend's concrete TxId.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum AbstractTxId {
    Loose(LooseTxId),
    Dense(DenseTxId),
}

/// Abstract transaction output ID that can represent any backend's concrete TxOutId.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum AbstractTxOutId {
    Loose(LooseTxOutId),
    Dense(DenseTxOutId),
}

/// Abstract transaction input ID that can represent any backend's concrete TxInId.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum AbstractTxInId {
    Loose(LooseTxInId),
    Dense(DenseTxInId),
}

// AbstractTxId conversions
impl From<LooseTxId> for AbstractTxId {
    fn from(id: LooseTxId) -> Self {
        Self::Loose(id)
    }
}

impl From<DenseTxId> for AbstractTxId {
    fn from(id: DenseTxId) -> Self {
        Self::Dense(id)
    }
}

impl AbstractTxId {
    pub fn try_as_loose(&self) -> Option<LooseTxId> {
        match self {
            Self::Loose(id) => Some(*id),
            Self::Dense(_) => None,
        }
    }

    pub fn try_as_dense(&self) -> Option<DenseTxId> {
        match self {
            Self::Loose(_) => None,
            Self::Dense(id) => Some(*id),
        }
    }
}

// AbstractTxOutId conversions
impl From<LooseTxOutId> for AbstractTxOutId {
    fn from(id: LooseTxOutId) -> Self {
        Self::Loose(id)
    }
}

impl From<DenseTxOutId> for AbstractTxOutId {
    fn from(id: DenseTxOutId) -> Self {
        Self::Dense(id)
    }
}

impl AbstractTxOutId {
    pub fn try_as_loose(&self) -> Option<LooseTxOutId> {
        match self {
            Self::Loose(id) => Some(*id),
            Self::Dense(_) => None,
        }
    }

    pub fn try_as_dense(&self) -> Option<DenseTxOutId> {
        match self {
            Self::Loose(_) => None,
            Self::Dense(id) => Some(*id),
        }
    }

    /// Returns the abstract transaction ID that contains this output.
    pub fn txid(&self) -> AbstractTxId {
        match self {
            Self::Loose(id) => AbstractTxId::Loose(id.txid()),
            Self::Dense(id) => AbstractTxId::Dense(id.txid()),
        }
    }
}

// AbstractTxInId conversions
impl From<LooseTxInId> for AbstractTxInId {
    fn from(id: LooseTxInId) -> Self {
        Self::Loose(id)
    }
}

impl From<DenseTxInId> for AbstractTxInId {
    fn from(id: DenseTxInId) -> Self {
        Self::Dense(id)
    }
}

impl AbstractTxInId {
    pub fn try_as_loose(&self) -> Option<LooseTxInId> {
        match self {
            Self::Loose(id) => Some(*id),
            Self::Dense(_) => None,
        }
    }

    pub fn try_as_dense(&self) -> Option<DenseTxInId> {
        match self {
            Self::Loose(_) => None,
            Self::Dense(id) => Some(*id),
        }
    }

    pub fn txid(&self) -> AbstractTxId {
        match self {
            Self::Loose(id) => AbstractTxId::Loose(id.txid()),
            Self::Dense(id) => AbstractTxId::Dense(id.txid()),
        }
    }
}
