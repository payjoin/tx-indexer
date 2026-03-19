#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum OutputType {
    P2pkh = 0,
    P2sh = 1,
    P2wpkh = 2,
    P2wsh = 3,
    P2tr = 4,
    OpReturn = 5,
    NonStandard = 6,
    // TODO: pay2anchor
}

impl OutputType {
    pub fn as_u32(self) -> u32 {
        self as u32
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum InputSortingType {
    Single = 0,
    Ascending = 1,
    Descending = 2,
    Bip69 = 3,
    Historical = 4,
    Unknown = 5,
}

impl InputSortingType {
    pub fn as_u32(self) -> u32 {
        self as u32
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum OutputStructureType {
    Single = 0,
    Double = 1,
    Multi = 2,
    Bip69 = 3,
}

impl OutputStructureType {
    pub fn as_u32(self) -> u32 {
        self as u32
    }
}
