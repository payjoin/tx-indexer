use colorous::PAIRED;
use graphviz_rust::{
    dot_generator::{attr, edge, graph, id, node, node_id, stmt, subgraph},
    dot_structures::{
        Attribute, Edge, EdgeTy, Graph, GraphAttributes, Id, Node, NodeId, Stmt, Subgraph, Vertex,
    },
};
use im::OrdSet;

use crate::{transaction::TxHandle, wallet::AddressId, Simulation};

/// The simulation always reserves the first address as the miner address
const MINER_ADDRESS: AddressId = AddressId(0);

// TODO make overridable? builder pattern?
pub fn new(name: &str) -> Graph {
    graph!(strict di id!(name);
        stmt!(GraphAttributes::Graph(vec![
            attr!("rankdir", "LR"),
            attr!("nodesep","0.05"),
            attr!("ranksep","0.02"),
            attr!("style","rounded"),
            attr!("bgcolor", "transparent"),
            attr!("color","\"#666666\""),
            attr!("penwidth","0.5"),
            attr!("labelloc","b"),
        ])),
        stmt!(GraphAttributes::Node(vec![
            attr!("fontname","helvetica"),
            attr!("fontsize","11"),
            attr!("shape","box"),
            attr!("style","\"rounded,filled\""),
            attr!("fillcolor","\"#ffffff\""),
            attr!("fontcolor","\"#000000\""),
            attr!("penwidth","2"),
            attr!("color","transparent"),
            attr!("width","0"),
            attr!("height","0"),
            attr!("margin","0.06"),
            attr!("pad", "0"),
            attr!("sep","1"),
        ])),
        stmt!(GraphAttributes::Edge(vec![
            attr!("color","\"#303030\""),
            attr!("minlen","3"),
            attr!("penwidth","1"),
            attr!("arrowsize","0.25"),
        ]))
    )
}

impl Simulation {
    fn is_miner_reward_tx(&self, tx_id: crate::transaction::TxId) -> bool {
        let tx = tx_id.with(self);
        tx.is_coinbase()
            && tx.outputs().all(|o| o.data().address_id == MINER_ADDRESS)
            && tx.outputs().next().is_some()
    }

    pub fn draw_tx_graph(&self) -> Graph {
        let mut graph = new("tx_graph");

        let hidden_txs: OrdSet<crate::transaction::TxId> = (0..self.tx_data.len())
            .map(crate::transaction::TxId)
            .filter(|&id| self.is_miner_reward_tx(id))
            .collect();

        let txs = self
            .tx_data
            .iter()
            .enumerate()
            .map(|(i, _)| crate::transaction::TxId(i))
            .filter(|id| !hidden_txs.contains(id));
        let mut txs: OrdSet<crate::transaction::TxId> = txs.into_iter().collect();
        if txs.is_empty() {
            // TODO default to broadcast txs, not all created txs?
            // 1.. to skip genesis coinbase
            txs = (1..self.tx_data.len())
                .map(crate::transaction::TxId)
                .filter(|id| !hidden_txs.contains(id))
                .collect();
        }

        for tx in &txs {
            graph.add_stmt(stmt!(self.draw_tx_cluster(*tx)));
        }

        for (txo, spending_inputs) in &self.spends {
            // Skip edges originating from hidden miner-reward transactions so
            // miner UTXOs don't appear as dangling dashed stubs.
            if hidden_txs.contains(&txo.txid) {
                continue;
            }
            let txo_id = format!("tx_{}_output_{}", txo.txid.0, txo.index);
            for txin in spending_inputs {
                if hidden_txs.contains(&txin.txid) {
                    continue;
                }
                let txo_included = txs.contains(&txo.txid);
                let txin_included = txs.contains(&txin.txid);
                if txo_included || txin_included {
                    let txin_id = format!("tx_{}_input_{}", txin.txid.0, txin.index);

                    if txo_included && txin_included {
                        graph.add_stmt(stmt!(edge!(node_id!(txo_id) => node_id!(txin_id))));
                    } else if !txo_included {
                        graph.add_stmt(stmt!(
                            edge!(node_id!(txo_id) => node_id!(txin_id); attr!("style", "dashed"))
                        ));
                        graph.add_stmt(stmt!(
                            node!(txo_id; attr!("style", "invis"), attr!("shape", "point"))
                        ));
                    } else if !txin_included {
                        graph.add_stmt(stmt!(edge!(node_id!(txo_id) => node_id!(txin_id); attr!("style", "dashed"), attr!("arrowhead", "none"))));
                        graph.add_stmt(stmt!(
                            node!(txin_id; attr!("style", "invis"), attr!("shape", "point"))
                        ));
                    }
                }
            }
        }

        for tx in txs.iter() {
            let tx = tx.with(self);
            tx.inputs().for_each(|input| {
                let input_id = format!("tx_{}_input_{}", tx.id.0, input.id.index);
                let wallet_id = input.prevout().wallet().id;
                let color =
                    format!("\"#{:x}\"", PAIRED[wallet_id.0 % PAIRED.len()]).to_ascii_uppercase();
                graph.add_stmt(stmt!(node!(input_id; attr!("fillcolor", color))));
            });
            tx.outputs().for_each(|output| {
                let output_id = format!("tx_{}_output_{}", tx.id.0, output.outpoint.index);
                let wallet_id = output.wallet().id;
                let color =
                    format!("\"#{:x}\"", PAIRED[wallet_id.0 % PAIRED.len()]).to_ascii_uppercase();

                graph.add_stmt(stmt!(node!(output_id; attr!("fillcolor", color))));
            });
        }
        // FIXME refactor (unconfirmed utxo query method)
        let last_block = self.block_info.last().unwrap();
        let last_bx = self.broadcast_set_info.last().unwrap();
        let unconfirmed_txos = last_bx.unconfirmed_txs.iter().cloned().flat_map(|tx| {
            tx.with(self)
                .outputs()
                .map(|o| o.outpoint)
                .collect::<Vec<_>>()
        });
        for utxo in last_block
            .utxos
            .iter()
            .cloned()
            .chain(unconfirmed_txos)
            .filter(|txo| txs.contains(&txo.txid))
            .filter(|txo| !self.spends.contains_key(txo))
        {
            let utxo_id = format!("tx_{}_output_{}", utxo.txid.0, utxo.index);
            graph.add_stmt(stmt!(node!(utxo_id; attr!("color", "\"#333333\""))))
        }

        graph
    }

    fn draw_input_cluster(&self, tx: &TxHandle) -> Subgraph {
        let id = format!("cluster_tx_{}_inputs", tx.id.0);
        subgraph!(
            id,
            std::iter::once(stmt!(GraphAttributes::Graph(vec![
                attr!("style", "invis"),
                attr!("margin", "0"),
                attr!("pad", "0")
            ])))
            .chain(tx.inputs().enumerate().map(|(idx, input)| {
                let id = format!("tx_{}_input_{}", tx.id.0, idx);
                let value = format!(
                    "\"{}\"",
                    format_min_hamming_weight_sats(input.prevout().data().amount.to_sat())
                );
                node!(id; attr!("label", value)).into()
            }))
            .collect()
        )
    }

    fn draw_output_cluster(&self, tx: &TxHandle) -> Subgraph {
        let id = format!("cluster_tx_{}_outputs", tx.id.0);
        subgraph!(
            id,
            std::iter::once(stmt!(GraphAttributes::Graph(vec![
                attr!("style", "invis"),
                attr!("margin", "0"),
                attr!("pad", "0")
            ])))
            .chain(tx.outputs().enumerate().map(|(idx, output)| {
                let node_id = format!("tx_{}_output_{}", tx.id.0, idx);
                let value = format!(
                    "\"{}\"",
                    format_min_hamming_weight_sats(output.data().amount.to_sat())
                ); // TODO format
                let node = node!(node_id; attr!("label", value));
                stmt!(node)
            }))
            .collect::<Vec<_>>()
        )
    }

    fn draw_tx_cluster(&self, tx: crate::transaction::TxId) -> Subgraph {
        let tx = tx.with(self);
        let outputs = self.draw_output_cluster(&tx);

        let tx_info = if tx.is_coinbase() {
            format!("\"tx #{}\ncoinbase\"", tx.id.0)
        } else {
            let tx_info = tx.info();
            format!(
                "\"tx #{}\n{} vB &times; {} sat/vB = {} sats in fees\"",
                tx.id.0,
                tx_info.weight.to_wu() as f32 / 4.0,
                tx_info.clone().feerate().to_sat_per_vb_ceil(),
                format_min_hamming_weight_sats(tx_info.fee.to_sat()),
            )
        };
        let tx_graph_attrs = stmt!(GraphAttributes::Graph(vec![
            attr!("bgcolor", "\"#d0d0d0\""),
            attr!("margin", "4"),
            attr!("ranksep", "0.02"),
            attr!("fontname", "helvetica"),
            attr!("fontsize", "6"),
            attr!("fontcolor", "\"#555555\""),
            attr!("tooltip", tx_info), // FIXME where is that goddamn margin coming from when this is a label instead of tooltip?
        ]));

        // is this supposed to be only on the invisible edge?
        let tx_edge_attrs = stmt!(GraphAttributes::Edge(vec![attr!("minlen", "1"),]));

        if tx.is_coinbase() {
            let id = format!("cluster_tx_{}", tx.id.0);
            subgraph!(id;
                tx_graph_attrs,
                tx_edge_attrs,
                      outputs)
        } else {
            let inputs = self.draw_input_cluster(&tx);
            let tx_subgraph_id = format!("cluster_tx_{}", tx.id.0);
            let first_input_id = format!("tx_{}_input_0", tx.id.0);
            let first_output_id = format!("tx_{}_output_0", tx.id.0);
            subgraph!(
                tx_subgraph_id;
                tx_graph_attrs,
                tx_edge_attrs,
                inputs,
                outputs,
                edge!(node_id!(first_input_id) => node_id!(first_output_id);
                      attr!("style", "invis"),
                      attr!("len", "0.01"),
                      attr!("constraint", "true")
                )
            )
        }
    }
}

pub fn format_min_hamming_weight_sats(n: u64) -> String {
    fn is_power_of_two(n: u64) -> bool {
        n > 0 && (n & (n - 1)) == 0
    }

    const fn largest_pow_under(base: u64, max: u64) -> u64 {
        let mut accum = 1;
        while accum < max {
            accum *= base;
        }

        accum
    }

    fn is_power_of_three(n: u64) -> bool {
        n > 0 && largest_pow_under(3, 2.1e15 as u64).is_multiple_of(n)
    }

    fn power_of_two_exponent(n: u64) -> u64 {
        let mut exp = 0;
        let mut value = n;
        while value > 1 {
            value >>= 1;
            exp += 1;
        }
        exp
    }

    fn power_of_three_exponent(n: u64) -> u64 {
        let mut exp = 0;
        let mut value = n;
        while value > 1 {
            value /= 3;
            exp += 1;
        }
        exp
    }

    fn unicode_exponent(exponent: u64) -> String {
        let superscripts = ['⁰', '¹', '²', '³', '⁴', '⁵', '⁶', '⁷', '⁸', '⁹'];
        exponent
            .to_string()
            .chars()
            .map(|c| superscripts[c.to_digit(10).unwrap() as usize])
            .collect::<String>()
    }

    // Check if the number is a power of two
    if is_power_of_two(n) {
        let exponent = power_of_two_exponent(n);
        format!("2{}", unicode_exponent(exponent))
    } else if is_power_of_three(n) {
        let exponent = power_of_three_exponent(n);
        format!("3{}", unicode_exponent(exponent))
    } else if is_power_of_three(n >> 1) {
        let exponent = power_of_three_exponent(n);
        format!("2×3{}", unicode_exponent(exponent))
    } else {
        // TODO refactor to make factor extraction the same code path for base 3 and base 10
        for factor in [1, 2, 5] {
            let mut value = factor;
            let mut exponent = 0;
            while value <= n {
                if value == n {
                    return if exponent == 0 {
                        format!("{}", factor)
                    } else if factor == 1 {
                        format!("10{}", unicode_exponent(exponent))
                    } else {
                        format!("{}×10{}", factor, unicode_exponent(exponent))
                    };
                }
                value *= 10;
                exponent += 1;
            }
        }

        n.to_string()
    }
}
