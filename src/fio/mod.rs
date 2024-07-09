pub mod file_io;
use std::path::PathBuf;

use file_io::FileIO;

use crate::errors::Result;
pub trait IOManager: Sync + Send {
    fn write(&self, buf: &[u8]) -> Result<usize>;
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    fn sync(&self) -> Result<()>;
}

pub fn new_io_manager(file_name: PathBuf) -> Result<impl IOManager> {
    FileIO::new(file_name)
}
