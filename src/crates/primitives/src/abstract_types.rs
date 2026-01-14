use bitcoin::Amount;

use crate::loose::{TxId, TxOutId};

// Anything transaction handle like
pub trait AbstractTxHandle {
    fn id(&self) -> TxId;
}

// Should be implemented by any type that is contained within a transaction.
pub trait TxConstituent {
    type Handle: AbstractTxHandle;
    fn containing_tx(&self) -> Self::Handle;

    fn index(&self) -> usize;
}

pub trait OutputCount: AbstractTxHandle {
    fn output_count(&self) -> usize;
}

pub trait EnumerateSpentTxOuts: AbstractTxHandle {
    fn spent_coins(&self) -> impl Iterator<Item = TxOutId>;
}

// TODO: find a better name for this
pub trait EnumerateOutputValueInArbitraryOrder: AbstractTxHandle {
    // TODO:
    fn output_values(&self) -> impl Iterator<Item = Amount>;
}
