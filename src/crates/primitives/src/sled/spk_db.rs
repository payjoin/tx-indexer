use crate::traits::ScriptPubkeyDb;
use crate::{ScriptPubkeyHash, dense::TxOutId};
use sled::{IVec, Tree};

pub const SPK_TREE_NAME: &str = "script_pubkey_index";
const OUT_ID_LEN: usize = 8;

#[derive(Debug)]
pub enum SledScriptPubkeyDbError {
    Backend(sled::Error),
    Serilaization(String),
}

impl std::error::Error for SledScriptPubkeyDbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SledScriptPubkeyDbError::Backend(e) => Some(e),
            SledScriptPubkeyDbError::Serilaization(_) => None,
        }
    }
}

impl std::fmt::Display for SledScriptPubkeyDbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SledScriptPubkeyDbError::Backend(e) => write!(f, "backend error: {}", e),
            SledScriptPubkeyDbError::Serilaization(e) => write!(f, "serialization error: {}", e),
        }
    }
}

pub struct SledScriptPubkeyDb {
    tree: Tree,
}

impl SledScriptPubkeyDb {
    pub fn from_tree(tree: Tree) -> Self {
        Self { tree }
    }

    fn key_bytes(spk_hash: &ScriptPubkeyHash) -> &[u8] {
        spk_hash.as_ref()
    }

    fn encode_out_id(out_id: TxOutId) -> IVec {
        IVec::from(out_id.index().to_le_bytes().to_vec())
    }

    fn decode_out_id(raw: &[u8]) -> Result<TxOutId, SledScriptPubkeyDbError> {
        if raw.len() != OUT_ID_LEN {
            return Err(SledScriptPubkeyDbError::Serilaization(format!(
                "expected {OUT_ID_LEN}-byte TxOutId, got {} bytes",
                raw.len()
            )));
        }
        let mut bytes = [0u8; OUT_ID_LEN];
        bytes.copy_from_slice(raw);
        Ok(TxOutId::new(u64::from_le_bytes(bytes)))
    }
}

impl ScriptPubkeyDb for SledScriptPubkeyDb {
    type Error = SledScriptPubkeyDbError;

    fn get(&self, spk_hash: &ScriptPubkeyHash) -> Result<Option<TxOutId>, Self::Error> {
        let key = Self::key_bytes(spk_hash);
        let value = self
            .tree
            .get(key)
            .map_err(|err| SledScriptPubkeyDbError::Backend(err))?;
        match value {
            Some(raw) => Ok(Some(Self::decode_out_id(raw.as_ref())?)),
            None => Ok(None),
        }
    }

    fn insert_if_absent(
        &mut self,
        spk_hash: ScriptPubkeyHash,
        out_id: TxOutId,
    ) -> Result<(), SledScriptPubkeyDbError> {
        let key = Self::key_bytes(&spk_hash);
        let value = Self::encode_out_id(out_id);
        let result = self
            .tree
            .compare_and_swap(key, None as Option<&[u8]>, Some(value))
            .map_err(|err| SledScriptPubkeyDbError::Backend(err))?;
        if result.is_err() {
            return Ok(());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_tree() -> (tempfile::TempDir, Tree) {
        let dir = tempfile::tempdir().unwrap();
        let db = sled::open(dir.path()).unwrap();
        let tree = db.open_tree(SPK_TREE_NAME).unwrap();
        (dir, tree)
    }

    fn spk_hash(bytes: [u8; 20]) -> ScriptPubkeyHash {
        bytes
    }

    #[test]
    fn get_missing_returns_none() {
        let (_dir, tree) = temp_tree();
        let db = SledScriptPubkeyDb::from_tree(tree);
        let key = spk_hash([1u8; 20]);
        assert!(matches!(db.get(&key), Ok(None)));
    }

    #[test]
    fn insert_then_get_returns_out_id() {
        let (_dir, tree) = temp_tree();
        let mut db = SledScriptPubkeyDb::from_tree(tree);
        let key = spk_hash([2u8; 20]);
        let out_id = TxOutId::new(100);

        db.insert_if_absent(key, out_id).unwrap();
        let got = db.get(&key).unwrap();
        assert_eq!(got, Some(out_id));
    }

    #[test]
    fn insert_if_absent_does_not_overwrite() {
        let (_dir, tree) = temp_tree();
        let mut db = SledScriptPubkeyDb::from_tree(tree);
        let key = spk_hash([3u8; 20]);
        let first = TxOutId::new(10);
        let second = TxOutId::new(20);

        db.insert_if_absent(key, first).unwrap();
        db.insert_if_absent(key, second).unwrap();
        let got = db.get(&key).unwrap();
        assert_eq!(got, Some(first));
    }

    #[test]
    fn different_spk_hashes_stored_separately() {
        let (_dir, tree) = temp_tree();
        let mut db = SledScriptPubkeyDb::from_tree(tree);
        let key_a = spk_hash([4u8; 20]);
        let key_b: ScriptPubkeyHash = {
            let mut b = [5u8; 20];
            b[0] = 4;
            b
        };
        let out_a = TxOutId::new(1);
        let out_b = TxOutId::new(2);

        db.insert_if_absent(key_a, out_a).unwrap();
        db.insert_if_absent(key_b, out_b).unwrap();
        assert_eq!(db.get(&key_a).unwrap(), Some(out_a));
        assert_eq!(db.get(&key_b).unwrap(), Some(out_b));
    }

    #[test]
    fn roundtrip_high_index() {
        let (_dir, tree) = temp_tree();
        let mut db = SledScriptPubkeyDb::from_tree(tree);
        let key = spk_hash([6u8; 20]);
        let out_id = TxOutId::new(u64::MAX);

        db.insert_if_absent(key, out_id).unwrap();
        assert_eq!(db.get(&key).unwrap(), Some(out_id));
    }
}
