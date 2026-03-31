#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
pub enum OutputStructureType {
    Single = 0,
    Double = 1,
    Multi = 2,
    Bip69 = 3,
    Unknown = 4,
}

impl OutputStructureType {
    pub fn as_u32(self) -> u32 {
        self as u32
    }
}
