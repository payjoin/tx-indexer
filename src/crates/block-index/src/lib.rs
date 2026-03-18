mod error;
mod varint;

pub use error::Error;

use std::io::Cursor;
use std::path::Path;

use rusty_leveldb::{DB, Options};

use varint::read_varint;

/// The LevelDB key for the obfuscation XOR key.
/// Bitcoin Core: `\x00obfuscate_key`.
const OBFUSCATE_KEY_KEY: &[u8] = b"\x00obfuscate_key";

/// Location of a block in the blk*.dat files, from a `'b'` + block_hash entry.
#[derive(Debug, Clone)]
pub struct BlockLocation {
    pub n_file: u32,
    pub data_pos: u32,
    pub n_tx: u32,
    pub height: u32,
}

impl BlockLocation {
    /// Construct the blk file path: `blocks_dir/blkNNNNN.dat`.
    pub fn blk_path(&self, blocks_dir: &Path) -> std::path::PathBuf {
        BlockIndex::blk_path(blocks_dir, self.n_file)
    }
}

/// Info about a single blk*.dat file, from an `'f'` + file_number entry.
#[derive(Debug, Clone)]
pub struct BlockFileInfo {
    pub n_blocks: u32,
    pub size: u32,
    pub undo_size: u32,
    pub height_first: u32,
    pub height_last: u32,
    pub time_first: u32,
    pub time_last: u32,
}

/// Thin read-only wrapper around Bitcoin Core's LevelDB block index.
///
/// Provides on-demand key lookups — no upfront indexing phase.
pub struct BlockIndex {
    db: DB,
    obfuscation_key: Vec<u8>,
}

impl BlockIndex {
    /// Open the block index LevelDB at the given path.
    ///
    /// `index_path` is typically `~/.bitcoin/blocks/index/`.
    pub fn open(index_path: &Path) -> Result<Self, Error> {
        let mut opts = Options::default();
        opts.create_if_missing = false;

        let mut db = DB::open(index_path, opts)?;

        // Read the obfuscation key (may be absent on very old Bitcoin Core installs).
        let obfuscation_key = db
            .get(OBFUSCATE_KEY_KEY)
            .map(|b| b.to_vec())
            .unwrap_or_default();

        Ok(Self {
            db,
            obfuscation_key,
        })
    }

    /// Last block file number (e.g. 4300 means blk04300.dat is the newest).
    ///
    /// Reads the `'l'` key from the index.
    pub fn last_block_file(&mut self) -> Result<u32, Error> {
        let raw = self.db.get(b"l").ok_or(Error::KeyNotFound("l"))?;
        let deobfuscated = self.deobfuscate(&raw);
        // Serialized as a 4-byte little-endian int.
        let bytes: [u8; 4] = deobfuscated.try_into().map_err(|_| Error::UnexpectedEof)?;
        Ok(u32::from_le_bytes(bytes))
    }

    /// Metadata for a given block file number.
    ///
    /// Reads the `'f'` + 4-byte-LE file number key.
    pub fn block_file_info(&mut self, file_number: u32) -> Result<BlockFileInfo, Error> {
        let mut key = Vec::with_capacity(5);
        key.push(b'f');
        key.extend_from_slice(&file_number.to_le_bytes());

        let raw = self.db.get(&key).ok_or(Error::KeyNotFound("f"))?;
        let deobfuscated = self.deobfuscate(&raw);
        parse_block_file_info(&deobfuscated)
    }

    /// Look up a block's location in the blk files by its hash.
    ///
    /// Reads the `'b'` + 32-byte block hash key (CDiskBlockIndex).
    pub fn block_location(&mut self, block_hash: &[u8; 32]) -> Result<BlockLocation, Error> {
        let mut key = Vec::with_capacity(33);
        key.push(b'b');
        key.extend_from_slice(block_hash);

        let raw = self.db.get(&key).ok_or(Error::KeyNotFound("b"))?;
        let deobfuscated = self.deobfuscate(&raw);
        parse_block_location(&deobfuscated)
    }

    /// Construct the blk file path: `blocks_dir/blkNNNNN.dat`.
    pub fn blk_path(blocks_dir: &Path, file_number: u32) -> std::path::PathBuf {
        blocks_dir.join(format!("blk{:05}.dat", file_number))
    }

    fn deobfuscate(&self, data: &[u8]) -> Vec<u8> {
        if self.obfuscation_key.is_empty() {
            return data.to_vec();
        }
        data.iter()
            .enumerate()
            .map(|(i, &b)| b ^ self.obfuscation_key[i % self.obfuscation_key.len()])
            .collect()
    }
}

/// Status flag: block data is stored in a blk*.dat file.
const BLOCK_HAVE_DATA: u32 = 8;
/// Status flag: undo data is stored in a rev*.dat file.
const BLOCK_HAVE_UNDO: u32 = 16;

/// Parse a CDiskBlockIndex value (after deobfuscation) into a BlockLocation.
///
/// Layout:
///   n_version: varint
///   n_height:  varint
///   n_status:  varint
///   n_tx:      varint
///   n_file:    varint  (only if BLOCK_HAVE_DATA | BLOCK_HAVE_UNDO)
///   data_pos:  varint  (only if BLOCK_HAVE_DATA)
///   undo_pos:  varint  (only if BLOCK_HAVE_UNDO)
///   header:    80 bytes (not needed here)
fn parse_block_location(data: &[u8]) -> Result<BlockLocation, Error> {
    let mut cursor = Cursor::new(data);

    let _n_version = read_varint(&mut cursor)?;
    let height = read_varint(&mut cursor)? as u32;
    let n_status = read_varint(&mut cursor)? as u32;
    let n_tx = read_varint(&mut cursor)? as u32;

    let n_file = if n_status & (BLOCK_HAVE_DATA | BLOCK_HAVE_UNDO) != 0 {
        read_varint(&mut cursor)? as u32
    } else {
        return Err(Error::BlockNotStored);
    };

    let data_pos = if n_status & BLOCK_HAVE_DATA != 0 {
        read_varint(&mut cursor)? as u32
    } else {
        return Err(Error::BlockNotStored);
    };

    Ok(BlockLocation {
        n_file,
        data_pos,
        n_tx,
        height,
    })
}

/// Parse a CBlockFileInfo value (after deobfuscation).
///
/// Layout: 7 consecutive Bitcoin Core VarInts:
///   nBlocks, nSize, nUndoSize, nHeightFirst, nHeightLast, nTimeFirst, nTimeLast
fn parse_block_file_info(data: &[u8]) -> Result<BlockFileInfo, Error> {
    let mut cursor = Cursor::new(data);
    Ok(BlockFileInfo {
        n_blocks: read_varint(&mut cursor)? as u32,
        size: read_varint(&mut cursor)? as u32,
        undo_size: read_varint(&mut cursor)? as u32,
        height_first: read_varint(&mut cursor)? as u32,
        height_last: read_varint(&mut cursor)? as u32,
        time_first: read_varint(&mut cursor)? as u32,
        time_last: read_varint(&mut cursor)? as u32,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blk_path_formatting() {
        let path = BlockIndex::blk_path(Path::new("/data/blocks"), 42);
        assert_eq!(path.to_str().unwrap(), "/data/blocks/blk00042.dat");
    }

    #[test]
    fn deobfuscate_identity_when_no_key() {
        let idx = BlockIndex {
            db: open_dummy_db(),
            obfuscation_key: vec![],
        };
        let data = vec![1, 2, 3, 4];
        assert_eq!(idx.deobfuscate(&data), data);
    }

    #[test]
    fn deobfuscate_xors_cycling() {
        let idx = BlockIndex {
            db: open_dummy_db(),
            obfuscation_key: vec![0xAB, 0xCD],
        };
        let data = vec![0x00, 0x00, 0xFF, 0xFF];
        let expected = vec![0xAB, 0xCD, 0x54, 0x32];
        assert_eq!(idx.deobfuscate(&data), expected);
    }

    #[test]
    fn parse_block_file_info_roundtrip() {
        // Encode 7 varints: all single-byte (value < 128).
        let data = vec![
            10, // n_blocks = 10
            200, 1,   // size (varint for a larger number)
            50,  // undo_size = 50
            0,   // height_first = 0
            100, // height_last = 100
            0x80, 0x00, // time_first (varint = 128)
            0x80, 0x01, // time_last  (varint = 129)
        ];
        // varint 200,1: first byte=200 (0xC8), continuation bit set.
        // n = (0 << 7) | (0xC8 & 0x7F) = 0x48 = 72, n += 1 = 73
        // second byte = 1 (0x01), no continuation.
        // n = (73 << 7) | 1 = 9345
        let info = parse_block_file_info(&data).unwrap();
        assert_eq!(info.n_blocks, 10);
        assert_eq!(info.size, 9345);
        assert_eq!(info.undo_size, 50);
        assert_eq!(info.height_first, 0);
        assert_eq!(info.height_last, 100);
        assert_eq!(info.time_first, 128);
        assert_eq!(info.time_last, 129);
    }

    #[test]
    fn parse_block_location_with_data() {
        // n_version=1, height=500000, status=HAVE_DATA(8)|valid(5)=13, n_tx=2000,
        // n_file=42, data_pos=12345
        let data = vec![
            0x00, // n_version = 0
            100,  // height = 100
            13,   // status = 13
            50,   // n_tx = 50
            3,    // n_file = 3
            99,   // data_pos = 99
        ];
        let loc = parse_block_location(&data).unwrap();
        assert_eq!(loc.height, 100);
        assert_eq!(loc.n_tx, 50);
        assert_eq!(loc.n_file, 3);
        assert_eq!(loc.data_pos, 99);
    }

    #[test]
    fn parse_block_location_no_data_errors() {
        // status = 5 (valid but no BLOCK_HAVE_DATA)
        let data = vec![
            0x00, // n_version
            100,  // height
            5,    // status = VALID_SCRIPTS only, no HAVE_DATA
            10,   // n_tx
        ];
        assert!(matches!(
            parse_block_location(&data),
            Err(Error::BlockNotStored)
        ));
    }

    fn open_dummy_db() -> DB {
        let opts = rusty_leveldb::in_memory();
        DB::open("test", opts).unwrap()
    }
}
