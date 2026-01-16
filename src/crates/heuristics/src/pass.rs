use std::{collections::HashMap, marker::PhantomData};

use tx_indexer_primitives::{
    disjoint_set::{DisJointSet, SparseDisjointSet},
    loose::TxOutId,
    test_utils::{DummyTxData, DummyTxOut},
};

use crate::{
    change_identification::NaiveChangeIdentificationHueristic,
    common_input::{MultiInputHeuristic, MultiInputResult},
};

type BoxIter<T> = Box<dyn Iterator<Item = T> + Send>;

/// An analysis pass: consumes itself and produces `O`.
/// `I` is the "declared input shape" (useful for type-level plumbing),
/// but in this simple model we only materialize the output.
trait AnalysisPass<I, O>: Sized {
    fn output(self) -> O;

    fn filter<F, O2>(self, filter: F) -> Filtered<Self, I, O, F>
    where
        F: FilterPass<O, O2>,
    {
        // filter.filter(self.output())
        Filtered {
            upstream: self,
            filter,
            _pd: PhantomData,
        }
    }

    fn map<M, O2>(self, map: M) -> Mapped<Self, I, O, M, O2>
    where
        M: MapPass<O, O2>,
    {
        Mapped {
            upstream: self,
            map,
            _pd: PhantomData,
        }
    }
}

/// A filter pass: transforms an input collection into an output collection.
trait FilterPass<I, O> {
    fn filter(self, input: I) -> O;
}

/// A map pass: transforms item type (or shape) of the iterator.
trait MapPass<I, O> {
    fn map(self, input: I) -> O;
}

/// The analysis pass produced by calling `.map()` on an upstream pass.
struct Mapped<P, I, O, M, O2> {
    upstream: P,
    map: M,
    _pd: PhantomData<(I, O, O2)>,
}

impl<P, I, O, M, O2> AnalysisPass<I, O2> for Mapped<P, I, O, M, O2>
where
    P: AnalysisPass<I, O>,
    M: MapPass<O, O2>,
{
    fn output(self) -> O2 {
        self.map.map(self.upstream.output())
    }
}

struct Filtered<P, I, O, F> {
    upstream: P,
    filter: F,
    _pd: PhantomData<(I, O)>,
}

impl<P, I, O, F> AnalysisPass<I, O> for Filtered<P, I, O, F>
where
    P: AnalysisPass<I, O>,
    F: FilterPass<O, O>,
{
    fn output(self) -> O {
        self.filter.filter(self.upstream.output())
    }
}

/// Generic filter pass built from a predicate:
/// BoxIter<T> -> BoxIter<T>
struct PredicateFilterPass<T, Pred> {
    pred: Pred,
    _pd: PhantomData<T>,
}

impl<T, Pred> PredicateFilterPass<T, Pred> {
    fn new(pred: Pred) -> Self {
        Self {
            pred,
            _pd: PhantomData,
        }
    }
}

impl<T, Pred> FilterPass<BoxIter<T>, BoxIter<T>> for PredicateFilterPass<T, Pred>
where
    T: Send + 'static,
    Pred: FnMut(&T) -> bool + Send + 'static,
{
    // TODO: should this be a an AnalysisPass where the output is a BoxIter<T>?
    fn filter(mut self, input: BoxIter<T>) -> BoxIter<T> {
        Box::new(input.filter(move |item| (self.pred)(item)))
    }
}

// --- Concrete types and passes

/// Source data set: `()` -> BoxIter<DummyTxData>
#[derive(Clone)]
struct EveryTx {
    txs: Vec<DummyTxData>,
}

impl AnalysisPass<(), BoxIter<DummyTxData>> for EveryTx {
    fn output(self) -> BoxIter<DummyTxData> {
        // TODO: consider storing Arc<[DummyTxData]> or draining ownership.
        Box::new(self.txs.into_iter())
    }
}

struct MultiInputMapPass {
    mih: MultiInputHeuristic,
}

impl MapPass<BoxIter<DummyTxData>, SparseDisjointSet<TxOutId>> for MultiInputMapPass {
    fn map(self, input: BoxIter<DummyTxData>) -> SparseDisjointSet<TxOutId> {
        let mut disjoint_set = SparseDisjointSet::default();
        for tx in input {
            match self.mih.temp_merge_prevouts(&tx) {
                MultiInputResult::Cluster(pairs) => {
                    for (a, b) in pairs {
                        disjoint_set.union(a, b);
                    }
                }
                _ => {}
            }
        }

        disjoint_set
    }
}

struct ChangeIdentificationMapPass {
    change_identification_heuristic: NaiveChangeIdentificationHueristic,
}

impl MapPass<BoxIter<DummyTxData>, HashMap<TxOutId, bool>> for ChangeIdentificationMapPass {
    fn map(self, input: BoxIter<DummyTxData>) -> HashMap<TxOutId, bool> {
        let mut map = HashMap::new();
        for tx in input {
            for (i, _amount) in tx.outputs_amounts.iter().enumerate() {
                let txout = DummyTxOut {
                    index: i,
                    containing_tx: tx.clone(),
                };
                let txout_id = txout.id();
                if self.change_identification_heuristic.temp_is_change(txout) {
                    map.insert(txout_id, true);
                } else {
                    map.insert(txout_id, false);
                }
            }
        }
        map
    }
}

/// Generic map pass:
/// BoxIter<T> -> BoxIter<U>
struct FnMapPass<T, U, F> {
    f: F,
    _pd: PhantomData<(T, U)>,
}

impl<T, U, F> FnMapPass<T, U, F> {
    fn new(f: F) -> Self {
        Self {
            f,
            _pd: PhantomData,
        }
    }
}

impl<T, U, F> MapPass<BoxIter<T>, BoxIter<U>> for FnMapPass<T, U, F>
where
    T: Send + 'static,
    U: Send + 'static,
    F: FnMut(T) -> U + Send + 'static,
{
    fn map(mut self, input: BoxIter<T>) -> BoxIter<U> {
        Box::new(input.map(move |item| (self.f)(item)))
    }
}

#[cfg(test)]
mod tests {
    use tx_indexer_primitives::loose::{TxId, TxOutId};

    use crate::{
        change_identification::NaiveChangeIdentificationHueristic,
        coinjoin_detection::NaiveCoinjoinDetection,
        common_input::MultiInputHeuristic,
        pass::{
            AnalysisPass, ChangeIdentificationMapPass, DummyTxData, EveryTx, MultiInputMapPass,
            PredicateFilterPass,
        },
    };

    #[test]
    fn test_filtered() {
        let src = EveryTx {
            txs: vec![
                // Coinbase 1
                DummyTxData {
                    id: TxId(0),
                    outputs_amounts: vec![100, 200, 300],
                    spent_coins: vec![],
                },
                // Coinbase 2
                DummyTxData {
                    id: TxId(1),
                    outputs_amounts: vec![100, 100],
                    spent_coins: vec![],
                },
                // Non-coinjoin spending the two coinbases
                DummyTxData {
                    id: TxId(2),
                    // Creating a change output and a payment output
                    outputs_amounts: vec![100, 200],
                    // Spending the vout = 0 of the two coinbases
                    spent_coins: vec![TxOutId::new(TxId(0), 0), TxOutId::new(TxId(1), 0)],
                },
                // Spending the change output
                DummyTxData {
                    id: TxId(3),
                    outputs_amounts: vec![100],
                    // Spending the change output
                    spent_coins: vec![TxOutId::new(TxId(2), 0)],
                },
                // Spending the payment output
                DummyTxData {
                    id: TxId(4),
                    outputs_amounts: vec![200],
                    // Spending the payment output
                    spent_coins: vec![TxOutId::new(TxId(2), 1)],
                },
            ],
        };

        let coinjoin_filtered = src
            .clone()
            .filter(PredicateFilterPass::new(|tx: &DummyTxData| {
                !NaiveCoinjoinDetection.is_coinjoin_temp_bool(tx)
            }))
            .map(MultiInputMapPass {
                mih: MultiInputHeuristic,
            });

        let change_identification_map = src.map(ChangeIdentificationMapPass {
            change_identification_heuristic: NaiveChangeIdentificationHueristic,
        });
    }

    // #[test]
    // fn test_pass() {
    // let src = EveryTx {
    //     txs: vec![
    //         // Coinbase 1
    //         DummyTxData {
    //             id: TxId(0),
    //             outputs_amounts: vec![100, 200, 300],
    //             spent_coins: vec![],
    //         },
    //         // Coinbase 2
    //         DummyTxData {
    //             id: TxId(1),
    //             outputs_amounts: vec![100, 100],
    //             spent_coins: vec![],
    //         },
    //         // Non-coinjoin spending the two coinbases
    //         DummyTxData {
    //             id: TxId(2),
    //             // Creating a change output and a payment output
    //             outputs_amounts: vec![100, 200],
    //             // Spending the vout = 0 of the two coinbases
    //             spent_coins: vec![TxOutId::new(TxId(0), 0), TxOutId::new(TxId(1), 0)],
    //         },
    //         // Spending the change output
    //         DummyTxData {
    //             id: TxId(3),
    //             outputs_amounts: vec![100],
    //             // Spending the change output
    //             spent_coins: vec![TxOutId::new(TxId(2), 0)],
    //         },
    //         // Spending the payment output
    //         DummyTxData {
    //             id: TxId(4),
    //             outputs_amounts: vec![200],
    //             // Spending the payment output
    //             spent_coins: vec![TxOutId::new(TxId(2), 1)],
    //         },
    //     ],
    // };

    // // Filter coinjoins OUT
    // // Create predicated filter pass
    // let coinjoin_filter_pass = PredicateFilterPass::new(|tx: &DummyTxData| {
    //     !NaiveCoinjoinDetection.is_coinjoin_temp_bool(tx)
    // });
    // let coinjoin_filtered = coinjoin_filter_pass.filter(src.clone().output());

    // let multi_input_pass = MultiInputMapPass {
    //     mih: MultiInputHeuristic,
    // };

    // let mut uf = multi_input_pass.map(coinjoin_filtered);
    // let txout0 = TxOutId::new(TxId(0), 0);
    // let txout1 = TxOutId::new(TxId(1), 0);

    // assert_eq!(uf.find(txout0), uf.find(txout1));

    // let change_identification_pass = ChangeIdentificationMapPass {
    //     change_identification_heuristic: NaiveChangeIdentificationHueristic,
    // };

    // let change_identification_map = change_identification_pass.map(src.output());

    // // Hard code this check / operation. take the spending txs change and cluster it with the coibase txouts
    // let change_txout = TxOutId::new(TxId(2), 1);
    // let change_txout_id = change_identification_map.get(&change_txout).unwrap();
    // if *change_txout_id {
    //     uf.union(change_txout, txout0);
    // }

    // assert_eq!(uf.find(change_txout), uf.find(txout0));
    // assert_eq!(uf.find(change_txout), uf.find(txout1));

    // Note: after `.filter(...)` you have BoxIter, not an AnalysisPass anymore.
    // If you want chaining to stay in AnalysisPass land, apply `.map(...)` *before*
    // calling `.output()`, i.e. use `.map(...)` on the pass itself:
}
