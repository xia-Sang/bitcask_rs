use std::path::PathBuf;

pub struct Options {
    pub dir_path: PathBuf,
    pub data_file_size: u64,
    pub sync_writes: bool,
}
