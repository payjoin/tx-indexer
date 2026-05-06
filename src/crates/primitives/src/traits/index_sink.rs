pub trait IndexSink {
    type Error;

    /// Called for each input in the current tx (before `on_transaction`).
    fn on_input(
        &mut self,
        vin: usize,
        prev_txid: &[u8; 32],
        prev_vout: u32,
    ) -> Result<(), Self::Error>;

    /// Called for each output in the current tx (before `on_transaction`).
    fn on_output(&mut self, vout: usize, script_pubkey: &[u8]) -> Result<(), Self::Error>;

    /// Called after all inputs/outputs of a tx have been visited.
    /// `tx_bytes` is the raw serialized transaction (same bytes the visitor walked).
    fn on_transaction(
        &mut self,
        txid: &[u8; 32],
        blk_file_no: u32,
        blk_file_off: u32,
        tx_len: u32,
        tx_bytes: &[u8],
    ) -> Result<(), Self::Error>;

    /// Called once per block, after all its transactions, with the per-block tx count.
    fn on_block_end(&mut self, block_tx_count: u64) -> Result<(), Self::Error>;
}
