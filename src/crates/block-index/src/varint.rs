use std::io::Read;

use crate::Error;

/// Decode a Bitcoin Core VarInt (NOT CompactSize).
///
/// Each byte uses 7 data bits + 1 continuation bit (MSB).
/// Non-terminal bytes have +1 adjustment to avoid redundant encodings.
pub fn read_varint(reader: &mut impl Read) -> Result<u64, Error> {
    let mut n: u64 = 0;
    loop {
        let mut buf = [0u8; 1];
        reader
            .read_exact(&mut buf)
            .map_err(|_| Error::UnexpectedEof)?;
        let byte = buf[0];
        n = (n << 7) | (byte & 0x7F) as u64;
        if byte & 0x80 != 0 {
            n += 1;
        } else {
            return Ok(n);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn decode_zero() {
        let mut cur = Cursor::new([0x00]);
        assert_eq!(read_varint(&mut cur).unwrap(), 0);
    }

    #[test]
    fn decode_one_byte() {
        // 127 = 0x7F
        let mut cur = Cursor::new([0x7F]);
        assert_eq!(read_varint(&mut cur).unwrap(), 127);
    }

    #[test]
    fn decode_two_bytes() {
        // 128: first byte = 0x80 (continuation), second byte = 0x00
        // n = 0, byte=0x80: n = (0<<7)|(0x80&0x7F) = 0, n += 1 = 1
        // byte=0x00: n = (1<<7)|(0x00&0x7F) = 128, done
        let mut cur = Cursor::new([0x80, 0x00]);
        assert_eq!(read_varint(&mut cur).unwrap(), 128);
    }

    #[test]
    fn decode_large() {
        // Encode/decode round-trip for a known value.
        // 16384: 0x80 0x80 0x00
        // byte=0x80: n=0, n+=1=1
        // byte=0x80: n=(1<<7)|0=128, n+=1=129
        // byte=0x00: n=(129<<7)|0=16512
        // Actually let's just verify specific byte sequences.
        let mut cur = Cursor::new([0x80, 0x80, 0x00]);
        assert_eq!(read_varint(&mut cur).unwrap(), 16512);
    }

    #[test]
    fn unexpected_eof() {
        let mut cur = Cursor::new([0x80]); // continuation but no next byte
        assert!(read_varint(&mut cur).is_err());
    }
}
