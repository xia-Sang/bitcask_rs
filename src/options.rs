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
impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: std::env::temp_dir().join("bitcask-rs"),
            data_file_size: 1024 * 1024 * 8,
            sync_writes: true,
            index_type: IndexType::BTree,
        }
    }
}
pub struct IteratorOptions {
    pub prefix: Vec<u8>,
    pub reverse: bool,
}
impl Default for IteratorOptions {
    fn default() -> Self {
        Self {
            prefix: Default::default(),
            reverse: Default::default(),
        }
    }
}
