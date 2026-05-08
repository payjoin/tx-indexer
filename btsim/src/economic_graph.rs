use petgraph::graph::UnGraph;
use rand::Rng;
use rustworkx_core::generators::barabasi_albert_graph;

use crate::wallet::WalletId;

// dictates how PaymentObligations are generated
#[derive(Debug, Clone)]
pub(crate) struct EconomicGraph<R>
where
    R: Rng + Sized,
{
    graph: UnGraph<WalletId, f64>,
    m: usize,
    rng: R,
}

impl<'a, R> EconomicGraph<R>
where
    R: Rng + Sized,
{
    pub(crate) fn new(m: usize, rng: R) -> Self {
        Self {
            graph: UnGraph::<WalletId, f64>::default(),
            m,
            rng,
        }
    }

    pub(crate) fn grow(&mut self, id: WalletId) {
        if self.graph.node_count() < self.m {
            // connect to the first node if it exists, forming a star graph
            if let Some(i) = self.graph.node_indices().next() {
                let j = self.graph.add_node(id);
                self.graph.add_edge(j, i, self.rng.random_range(0.0..1.0)); // TODO 1.0 is too high
            } else {
                self.graph.add_node(id);
            }
        } else {
            let seed = self.rng.next_u64();

            // since the resulting RNG state from seeding is discarded at the
            // end of the barabasi albert generator, incrementally growing the
            // graph would require taking the same increments. the simplest way
            // to do that is to always set the increment to 1 node. this is less
            // efficient but more easy to make reproducible and efficiency
            // doesn't matter much for this structure
            self.graph = barabasi_albert_graph(
                self.graph.node_count() + 1,
                self.m,
                Some(seed),
                Some(std::mem::take(&mut self.graph)),
                || id,                              // only used once, hopefully
                || self.rng.random_range(0.0..1.0), // TODO 1.0 is too high
            )
            .expect("attaching new node to the graph should unconditionally succeed");
        }
    }

    // this is independent of any simulation state, it's just a directed flow
    // since the graph is a preferential attachment model this is supposed to
    // settle on a power law stationary distribution of wealth
    pub(crate) fn next_ordered_payment_pairs(
        &'a mut self,
    ) -> impl Iterator<Item = (WalletId, WalletId)> + 'a {
        self.graph.edge_indices().flat_map(|i| {
            if self.rng.random_bool(*self.graph.edge_weight(i).unwrap()) {
                // can default to 0.0 but this shouldn't happen
                let (a, b) = self.graph.edge_endpoints(i).unwrap();
                let (from, to) = if self.rng.random_bool(0.5) {
                    (a, b)
                } else {
                    (b, a)
                };
                Some((WalletId(from.index()), WalletId(to.index())))
            } else {
                None
            }
        })
    }
}
