use std::{
    fs::File,
    io::{self, BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

const BLOCK_HEADER_LEN: usize = 8;

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

    /// Stream blocks from a blk file one at a time, yielding `(block_start_offset, block_bytes)`.
    ///
    /// Never reads more than one block into memory at once — no 128 MB allocation.
    /// Stops at `data_len` bytes when provided (from the LevelDB block-index hint).
    pub fn iter_blocks(
        &self,
        file_no: u32,
        data_len: Option<usize>,
    ) -> impl Iterator<Item = io::Result<(u64, Vec<u8>)>> + '_ {
        let limit = data_len.unwrap_or(usize::MAX);
        // Encode the file-open error as `Err(Some(e))` so the closure can yield it on the
        // first `next()` call without a separate code path.
        let mut state: Result<BufReader<File>, Option<io::Error>> =
            File::open(self.blk_path(file_no))
                .map(BufReader::new)
                .map_err(Some);
        let mut file_offset: usize = 0;
        let mut done = false;

        std::iter::from_fn(move || {
            if done {
                return None;
            }
            let reader = match &mut state {
                Err(e_opt) => return e_opt.take().map(Err),
                Ok(r) => r,
            };
            if file_offset + BLOCK_HEADER_LEN > limit {
                return None;
            }
            let mut header = [0u8; BLOCK_HEADER_LEN];
            match reader.read_exact(&mut header) {
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return None,
                Err(e) => {
                    done = true;
                    return Some(Err(e));
                }
                Ok(()) => {}
            }
            self.apply_xor(&mut header, file_offset);
            let block_size =
                u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
            file_offset += BLOCK_HEADER_LEN;
            let block_start = file_offset as u64;
            let block_end = file_offset + block_size;
            if block_end > limit {
                done = true;
                return Some(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "block exceeds data_len",
                )));
            }
            let mut block_buf = vec![0u8; block_size];
            if let Err(e) = reader.read_exact(&mut block_buf) {
                done = true;
                return Some(Err(e));
            }
            self.apply_xor(&mut block_buf, file_offset);
            file_offset = block_end;
            Some(Ok((block_start, block_buf)))
        })
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
            Ok(_) => Vec::new(),
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    Vec::new()
                } else {
                    // Everything else is an unexpected error.
                    panic!("failed to read xor.dat: {}", e);
                }
            }
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
