#[repr(transparent)]
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct BlockHeight(pub u32);

#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct TxId {
    block_height: BlockHeight,
    byte_offset: u16,
}

#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct TxOutId {
    tx_id: TxId,
    byte_offset: u16,
}

impl TxOutId {
    pub fn txid(&self) -> TxId {
        self.tx_id
    }
}

#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct TxInId {
    tx_id: TxId,
    byte_offset: u16,
}

impl TxInId {
    pub fn txid(&self) -> TxId {
        self.tx_id
    }
}
