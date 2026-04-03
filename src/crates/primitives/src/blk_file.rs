use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

/// XOR-aware reader for Bitcoin Core blk*.dat files.
///
/// Bitcoin Core v27+ writes `blocks/xor.dat` containing an 8-byte key and XOR-encrypts
/// every blk file with it (cycling the key by absolute byte position).  This struct reads
/// the key once on construction and applies it transparently in all read operations.
/// When `xor.dat` is absent or all-zero the reads are pass-through.
pub struct BlkFileStore {
    blocks_dir: PathBuf,
    /// Cyclic XOR key.  Empty == no decryption.
    xor_key: Vec<u8>,
}

impl BlkFileStore {
    /// Open a block file store rooted at `blocks_dir`, reading `xor.dat` if present.
    pub fn open(blocks_dir: impl Into<PathBuf>) -> Self {
        let blocks_dir = blocks_dir.into();
        let xor_key = Self::read_xor_key(&blocks_dir);
        Self {
            blocks_dir,
            xor_key,
        }
    }

    pub fn blocks_dir(&self) -> &Path {
        &self.blocks_dir
    }

    /// Construct the path `blocks_dir/blkNNNNN.dat` for a given file number.
    pub fn blk_path(&self, file_no: u32) -> PathBuf {
        self.blocks_dir.join(format!("blk{:05}.dat", file_no))
    }

    /// Read an entire blk file into memory, XOR-decrypting from offset 0.
    pub fn read_file(&self, file_no: u32) -> io::Result<Vec<u8>> {
        let mut bytes = std::fs::read(self.blk_path(file_no))?;
        self.apply_xor(&mut bytes, 0);
        Ok(bytes)
    }

    /// Read `len` bytes starting at `offset` from a blk file, XOR-decrypting in place.
    pub fn read_at(&self, file_no: u32, offset: u32, len: u32) -> io::Result<Vec<u8>> {
        let mut f = File::open(self.blk_path(file_no))?;
        f.seek(SeekFrom::Start(offset as u64))?;
        let mut buf = vec![0u8; len as usize];
        f.read_exact(&mut buf)?;
        self.apply_xor(&mut buf, offset as usize);
        Ok(buf)
    }

    /// XOR-decrypt `data` whose first byte is at absolute file position `file_offset`.
    fn apply_xor(&self, data: &mut [u8], file_offset: usize) {
        if self.xor_key.is_empty() {
            return;
        }
        let key_len = self.xor_key.len();
        for (i, byte) in data.iter_mut().enumerate() {
            *byte ^= self.xor_key[(file_offset + i) % key_len];
        }
    }

    /// Read `blocks/xor.dat`.  Returns an empty `Vec` when the file is absent (pre-v27
    /// nodes) or contains all zeros (regtest fixtures), so callers can use `is_empty()`
    /// as a fast-path skip.
    fn read_xor_key(blocks_dir: &Path) -> Vec<u8> {
        match std::fs::read(blocks_dir.join("xor.dat")) {
            Ok(key) if key.iter().any(|&b| b != 0) => key,
            _ => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_file(path: &Path, data: &[u8]) {
        let mut f = File::create(path).unwrap();
        f.write_all(data).unwrap();
    }

    #[test]
    fn no_xor_dat_is_passthrough() {
        let tmp = tempfile::tempdir().unwrap();
        let store = BlkFileStore::open(tmp.path());
        let data = b"hello world";
        write_file(&store.blk_path(0), data);
        assert_eq!(store.read_file(0).unwrap(), data);
        assert_eq!(store.read_at(0, 6, 5).unwrap(), b"world");
    }

    #[test]
    fn all_zero_xor_dat_is_passthrough() {
        let tmp = tempfile::tempdir().unwrap();
        write_file(&tmp.path().join("xor.dat"), &[0u8; 8]);
        let store = BlkFileStore::open(tmp.path());
        assert!(
            store.xor_key.is_empty(),
            "all-zero key should be treated as absent"
        );
        let data = b"hello world";
        write_file(&store.blk_path(0), data);
        assert_eq!(store.read_file(0).unwrap(), data);
    }

    #[test]
    fn xor_key_decrypts_read_file() {
        let tmp = tempfile::tempdir().unwrap();
        let key = [0x01u8, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        write_file(&tmp.path().join("xor.dat"), &key);
        let plaintext = b"ABCDEFGHIJ"; // 10 bytes
        let encrypted: Vec<u8> = plaintext
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ key[i % 8])
            .collect();
        write_file(&tmp.path().join("blk00000.dat"), &encrypted);
        let store = BlkFileStore::open(tmp.path());
        assert_eq!(store.read_file(0).unwrap(), plaintext);
    }

    #[test]
    fn xor_key_decrypts_read_at_with_correct_phase() {
        let tmp = tempfile::tempdir().unwrap();
        let key = [0xAAu8, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22];
        write_file(&tmp.path().join("xor.dat"), &key);
        let plaintext = b"0123456789ABCDEF"; // 16 bytes
        let encrypted: Vec<u8> = plaintext
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ key[i % 8])
            .collect();
        write_file(&tmp.path().join("blk00000.dat"), &encrypted);
        let store = BlkFileStore::open(tmp.path());
        // Read bytes [4..12] — must use key phase starting at offset 4
        let got = store.read_at(0, 4, 8).unwrap();
        assert_eq!(got, &plaintext[4..12]);
    }
}
