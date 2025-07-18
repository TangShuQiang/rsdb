use crate::error::Result;

pub mod engine;
pub mod memory;

#[derive(Clone)]
pub struct Mvcc {}

impl Mvcc {
    pub fn new() -> Self {
        Self {}
    }

    pub fn begin(&self) -> Result<MvccTransaction> {
        Ok(MvccTransaction::new())
    }
}

pub struct MvccTransaction {}

impl MvccTransaction {
    pub fn new() -> Self {
        Self {}
    }
}
