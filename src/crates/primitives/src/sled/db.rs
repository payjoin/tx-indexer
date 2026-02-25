use std::path::Path;

use crate::sled::spk_db::{SPK_TREE_NAME, SledScriptPubkeyDb};
use sled::Db;

pub struct SledDBFactory {
    db: Db,
}

impl SledDBFactory {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }

    pub fn spk_db(&self) -> Result<SledScriptPubkeyDb, sled::Error> {
        Ok(SledScriptPubkeyDb::from_tree(
            self.db.open_tree(SPK_TREE_NAME)?,
        ))
    }
}
