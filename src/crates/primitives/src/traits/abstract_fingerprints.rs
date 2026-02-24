use crate::traits::abstract_types::AbstractTransaction;

pub trait HasNLockTime: AbstractTransaction {
    fn n_locktime(&self) -> u32;
}

// TODO: implement something similar
// pub trait FingerPrintVector: HasNLockTime {
//     fn fingerprint_vector(&self) -> Vec<u8> {
//         unimplemented!("some normalized vector of fingerprintss")
//     }
// }
