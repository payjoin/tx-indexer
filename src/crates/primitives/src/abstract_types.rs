use bitcoin::Amount;

use crate::loose::TxOutId;

// Anything transaction handle like
pub trait AbstractTxHandle {}

pub trait TxConstituent {
    type Handle: AbstractTxHandle;
    fn containing_tx(&self) -> Self::Handle;

    fn index(&self) -> usize;
}

pub trait OutputCount: AbstractTxHandle {
    fn output_count(&self) -> usize;
}

pub trait EnumerateSpentTxOuts: AbstractTxHandle {
    // TODO: do iterator later (maybe)
    // TODO:  handle?
    fn spent_coins(&self) -> Vec<TxOutId>;
}

// TODO: find a better name for this
pub trait EnumerateOutputValueInArbitraryOrder: AbstractTxHandle {
    // TODO:
    fn output_values(&self) -> impl Iterator<Item = Amount>;
}
