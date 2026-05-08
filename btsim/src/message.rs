use crate::{bulletin_board::BulletinBoardId, transaction::InputId, wallet::WalletId, TimeStep};

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct UTXORegisteration {
    pub(crate) input_id: InputId,
    pub(crate) owner: WalletId,
    pub(crate) valid_till: TimeStep,
}

#[derive(Debug, PartialEq, Eq, Clone)]
#[allow(dead_code)]
pub(crate) enum MessageType {
    /// Initiate a multi-party payjoin (shared bulletin board id for all participants).
    ProposeCoSpend(BulletinBoardId),
    /// Register a input in the order book
    RegisterWalletInput(UTXORegisteration),
}

define_entity!(
    Message,
    {
        pub(crate) id: MessageId,
        pub(crate) message: MessageType,
        pub(crate) from: WalletId,
        // None if meant as a broadcast message
        pub(crate) to: WalletId,
    },
    {
    }
);
define_entity_handle_mut!(Message);

impl<'a> MessageHandle<'a> {
    #[allow(dead_code)]
    pub(crate) fn data(&self) -> &'a MessageData {
        &self.sim.messages[self.id.0]
    }
}

impl<'a> MessageHandleMut<'a> {
    #[allow(dead_code)]
    pub(crate) fn post(&mut self, message: MessageData) {
        self.sim.messages.push(message);
    }
}
