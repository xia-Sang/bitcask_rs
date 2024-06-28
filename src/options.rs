use std::path::PathBuf;

#[derive(Clone)]
pub struct Options {
    pub dir_path: PathBuf,
    pub data_file_size: u64,
    pub sync_writes: bool,
    pub index_type: IndexType,
}
#[derive(Clone)]
pub enum IndexType {
    BTree,
    SkipList,
}
