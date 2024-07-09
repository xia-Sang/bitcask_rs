pub mod btree;

use bytes::Bytes;

use crate::{
    data::log_record::LogRecordPos,
    errors::Result,
    options::{IndexType, IteratorOptions},
};

pub trait Indexer: Sync + Send {
    /// 实现put方法
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;
    /// 实现get方法
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;
    /// 实现delete方法
    fn delete(&self, key: Vec<u8>) -> bool;
    fn list_keys(&self) -> Result<Vec<Bytes>>;
    /// 实现iterator方法
    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator>;
}
pub fn new_index(index_type: IndexType) -> impl Indexer {
    match index_type {
        IndexType::BTree => btree::Btree::new(),
        IndexType::SkipList => todo!(),
    }
}
pub trait IndexIterator: Sync + Send {
    fn rewind(&mut self);
    fn seek(&mut self, key: Vec<u8>);
    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)>;
}
