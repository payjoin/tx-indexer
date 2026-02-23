use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::dense::TxId;

const TXPTR_LEN_BYTES: u64 = 24;
const BLOCK_TX_END_LEN_BYTES: u64 = 4;
const LINK_LEN_BYTES: u64 = 8;

pub const OUTID_NONE: u64 = u64::MAX;
pub const INID_NONE: u64 = u64::MAX;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TxPtr {
    blk_file_no: u32,
    blk_file_off: u32,
    tx_in_end: u64,
    tx_out_end: u64,
}

impl TxPtr {
    pub fn new(blk_file_no: u32, blk_file_off: u32, tx_in_end: u64, tx_out_end: u64) -> Self {
        Self {
            blk_file_no,
            blk_file_off,
            tx_in_end,
            tx_out_end,
        }
    }

    pub fn blk_file_no(self) -> u32 {
        self.blk_file_no
    }

    pub fn blk_file_off(self) -> u32 {
        self.blk_file_off
    }

    pub fn tx_in_end(self) -> u64 {
        self.tx_in_end
    }

    pub fn tx_out_end(self) -> u64 {
        self.tx_out_end
    }

    fn to_le_bytes(self) -> [u8; 24] {
        let mut out = [0u8; 24];
        out[..4].copy_from_slice(&self.blk_file_no.to_le_bytes());
        out[4..8].copy_from_slice(&self.blk_file_off.to_le_bytes());
        out[8..16].copy_from_slice(&self.tx_in_end.to_le_bytes());
        out[16..].copy_from_slice(&self.tx_out_end.to_le_bytes());
        out
    }

    fn from_le_bytes(bytes: [u8; 24]) -> Self {
        let blk_file_no = u32::from_le_bytes(bytes[..4].try_into().expect("slice length"));
        let blk_file_off = u32::from_le_bytes(bytes[4..8].try_into().expect("slice length"));
        let tx_in_end = u64::from_le_bytes(bytes[8..16].try_into().expect("slice length"));
        let tx_out_end = u64::from_le_bytes(bytes[16..].try_into().expect("slice length"));
        Self {
            blk_file_no,
            blk_file_off,
            tx_in_end,
            tx_out_end,
        }
    }
}

#[derive(Debug)]
pub struct ConfirmedTxPtrIndex {
    file: File,
    len: u64,
}

#[derive(Debug)]
pub struct BlockTxIndex {
    file: File,
    len: u64,
}

#[derive(Debug)]
pub struct InPrevoutIndex {
    file: File,
    len: u64,
}

#[derive(Debug)]
pub struct OutSpentByIndex {
    file: File,
    len: u64,
}

impl ConfirmedTxPtrIndex {
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Self { file, len: 0 })
    }

    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let len_bytes = file.metadata()?.len();
        if len_bytes % TXPTR_LEN_BYTES != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "confirmed tx ptr file length is not a multiple of 8 bytes",
            ));
        }
        Ok(Self {
            file,
            len: len_bytes / TXPTR_LEN_BYTES,
        })
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn append(&mut self, ptr: TxPtr) -> io::Result<TxId> {
        if self.len > u32::MAX as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "confirmed tx ptr index exceeds u32::MAX entries",
            ));
        }
        let txid = TxId::new(self.len as u32);
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&ptr.to_le_bytes())?;
        self.len += 1;
        Ok(txid)
    }

    pub fn get(&self, txid: TxId) -> io::Result<Option<TxPtr>> {
        let index = txid.index() as u64;
        if index >= self.len {
            return Ok(None);
        }
        let offset = index * TXPTR_LEN_BYTES;
        let mut buf = [0u8; 24];
        self.file.read_exact_at(&mut buf, offset)?;
        Ok(Some(TxPtr::from_le_bytes(buf)))
    }
}

impl BlockTxIndex {
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Self { file, len: 0 })
    }

    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let len_bytes = file.metadata()?.len();
        if len_bytes % BLOCK_TX_END_LEN_BYTES != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "block tx end file length is not a multiple of 4 bytes",
            ));
        }
        Ok(Self {
            file,
            len: len_bytes / BLOCK_TX_END_LEN_BYTES,
        })
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn last(&self) -> io::Result<Option<u32>> {
        if self.len == 0 {
            return Ok(None);
        }
        let offset = (self.len - 1) * BLOCK_TX_END_LEN_BYTES;
        let mut buf = [0u8; 4];
        self.file.read_exact_at(&mut buf, offset)?;
        Ok(Some(u32::from_le_bytes(buf)))
    }

    pub fn append(&mut self, block_tx_end: u32) -> io::Result<u64> {
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&block_tx_end.to_le_bytes())?;
        self.len += 1;
        Ok(self.len - 1)
    }

    pub fn get(&self, height: u64) -> io::Result<Option<u32>> {
        if height >= self.len {
            return Ok(None);
        }
        let offset = height * BLOCK_TX_END_LEN_BYTES;
        let mut buf = [0u8; 4];
        self.file.read_exact_at(&mut buf, offset)?;
        Ok(Some(u32::from_le_bytes(buf)))
    }
}

impl InPrevoutIndex {
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Self { file, len: 0 })
    }

    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let len_bytes = file.metadata()?.len();
        if len_bytes % LINK_LEN_BYTES != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "in_prevout_outid file length is not a multiple of 8 bytes",
            ));
        }
        Ok(Self {
            file,
            len: len_bytes / LINK_LEN_BYTES,
        })
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn append(&mut self, out_id: u64) -> io::Result<u64> {
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&out_id.to_le_bytes())?;
        self.len += 1;
        Ok(self.len - 1)
    }

    pub fn get(&self, in_id: u64) -> io::Result<Option<u64>> {
        if in_id >= self.len {
            return Ok(None);
        }
        let offset = in_id * LINK_LEN_BYTES;
        let mut buf = [0u8; 8];
        self.file.read_exact_at(&mut buf, offset)?;
        Ok(Some(u64::from_le_bytes(buf)))
    }
}

impl OutSpentByIndex {
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Self { file, len: 0 })
    }

    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let len_bytes = file.metadata()?.len();
        if len_bytes % LINK_LEN_BYTES != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "out_spent_by_inid file length is not a multiple of 8 bytes",
            ));
        }
        Ok(Self {
            file,
            len: len_bytes / LINK_LEN_BYTES,
        })
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn append(&mut self, in_id: u64) -> io::Result<u64> {
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&in_id.to_le_bytes())?;
        self.len += 1;
        Ok(self.len - 1)
    }

    pub fn set(&mut self, out_id: u64, in_id: u64) -> io::Result<()> {
        let offset = out_id * LINK_LEN_BYTES;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&in_id.to_le_bytes())
    }

    pub fn get(&self, out_id: u64) -> io::Result<Option<u64>> {
        if out_id >= self.len {
            return Ok(None);
        }
        let offset = out_id * LINK_LEN_BYTES;
        let mut buf = [0u8; 8];
        self.file.read_exact_at(&mut buf, offset)?;
        Ok(Some(u64::from_le_bytes(buf)))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BlockTxIndex, ConfirmedTxPtrIndex, INID_NONE, InPrevoutIndex, OutSpentByIndex, TxPtr,
    };
    use crate::dense::TxId;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_nanos();
        std::env::temp_dir().join(format!("confirmed_txptr_{}.bin", nanos))
    }

    #[test]
    fn append_and_read_round_trip() {
        let path = temp_path();
        let mut index = ConfirmedTxPtrIndex::create(&path).expect("create");
        let tx0 = index.append(TxPtr::new(1, 10, 3, 5)).expect("append");
        let tx1 = index.append(TxPtr::new(2, 20, 6, 9)).expect("append");

        assert_eq!(tx0, TxId::new(0));
        assert_eq!(tx1, TxId::new(1));
        assert_eq!(index.len(), 2);
        assert_eq!(
            index.get(TxId::new(0)).expect("get"),
            Some(TxPtr::new(1, 10, 3, 5))
        );

        drop(index);

        let reopened = ConfirmedTxPtrIndex::open(&path).expect("open");
        assert_eq!(reopened.len(), 2);
        assert_eq!(
            reopened.get(TxId::new(1)).expect("get"),
            Some(TxPtr::new(2, 20, 6, 9))
        );

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn block_tx_end_round_trip() {
        let path = temp_path();
        let mut index = BlockTxIndex::create(&path).expect("create");
        let h0 = index.append(2).expect("append");
        let h1 = index.append(5).expect("append");

        assert_eq!(h0, 0);
        assert_eq!(h1, 1);
        assert_eq!(index.len(), 2);
        assert_eq!(index.get(0).expect("get"), Some(2));

        drop(index);

        let reopened = BlockTxIndex::open(&path).expect("open");
        assert_eq!(reopened.len(), 2);
        assert_eq!(reopened.get(1).expect("get"), Some(5));

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn in_prevout_round_trip() {
        let path = temp_path();
        let mut index = InPrevoutIndex::create(&path).expect("create");
        let in0 = index.append(42).expect("append");
        let in1 = index.append(99).expect("append");

        assert_eq!(in0, 0);
        assert_eq!(in1, 1);
        assert_eq!(index.len(), 2);
        assert_eq!(index.get(0).expect("get"), Some(42));

        drop(index);

        let reopened = InPrevoutIndex::open(&path).expect("open");
        assert_eq!(reopened.len(), 2);
        assert_eq!(reopened.get(1).expect("get"), Some(99));

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn out_spent_by_round_trip() {
        let path = temp_path();
        let mut index = OutSpentByIndex::create(&path).expect("create");
        let out0 = index.append(INID_NONE).expect("append");
        let out1 = index.append(INID_NONE).expect("append");

        assert_eq!(out0, 0);
        assert_eq!(out1, 1);
        index.set(1, 7).expect("set");
        assert_eq!(index.get(1).expect("get"), Some(7));

        drop(index);

        let reopened = OutSpentByIndex::open(&path).expect("open");
        assert_eq!(reopened.get(0).expect("get"), Some(INID_NONE));
        assert_eq!(reopened.get(1).expect("get"), Some(7));

        let _ = fs::remove_file(&path);
    }
}
