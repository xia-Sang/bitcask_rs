pub mod btree;

use crate::{data::log_record::LogRecordPos, options::IndexType};

pub trait Indexer: Sync + Send {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;
    fn delete(&self, key: Vec<u8>) -> bool;
}
pub fn new_index(index_type: IndexType) -> impl Indexer {
    match index_type {
        IndexType::BTree => btree::Btree::new(),
        IndexType::SkipList => todo!(),
    }
}
