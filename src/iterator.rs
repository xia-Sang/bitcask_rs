use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{db::Engine, errors::Result, index::IndexIterator, options::IteratorOptions};

pub struct Iterator<'a> {
    index_iter: Arc<RwLock<Box<dyn IndexIterator>>>,
    engine: &'a Engine,
}
impl Engine {
    pub fn iter(&self, options: IteratorOptions) -> Iterator {
        Iterator {
            index_iter: Arc::new(RwLock::new(self.index.iterator(options))),
            engine: self,
        }
    }
    pub fn list_keys(&self) -> Result<Vec<Bytes>> {
        self.index.list_keys()
    }
    pub fn fold<F>(&self, f: F) -> Result<()>
    where
        Self: Sized,
        F: Fn(Bytes, Bytes) -> bool,
    {
        let iter = self.iter(IteratorOptions::default());
        while let Some((key, value)) = iter.next() {
            if !f(key, value) {
                break;
            }
        }
        Ok(())
    }
}
impl Iterator<'_> {
    pub fn rewind(&mut self) {
        let mut index_iter = self.index_iter.write();
        index_iter.rewind();
    }
    pub fn seek(&mut self, key: Vec<u8>) {
        let mut index_iter = self.index_iter.write();
        index_iter.seek(key)
    }
    pub fn next(&self) -> Option<(Bytes, Bytes)> {
        let mut index_iter = self.index_iter.write();
        if let Some(item) = index_iter.next() {
            let value = self
                .engine
                .get_value_by_position(item.1)
                .expect("faild to get value from data file");
            return Some((Bytes::from(item.0.to_vec()), value));
        }
        None
    }
}
#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{options::Options, util::rand_kv};

    use super::*;
    #[test]
    fn test_list_keys() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("./tmp/bitcask-rs-list_key");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let key1 = engine.list_keys();
        assert_eq!(key1.ok().unwrap().len(), 0);

        let put_res1 = engine.put(
            Bytes::from("xia".as_bytes().to_vec()),
            rand_kv::get_test_value(1),
        );
        assert!(put_res1.is_ok());

        let put_res2 = engine.put(
            Bytes::from("sang".as_bytes().to_vec()),
            rand_kv::get_test_value(2),
        );
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(
            Bytes::from("shi".as_bytes().to_vec()),
            rand_kv::get_test_value(3),
        );
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(
            Bytes::from("wo".as_bytes().to_vec()),
            rand_kv::get_test_value(4),
        );
        assert!(put_res4.is_ok());

        println!("{:?}", engine.list_keys());
        std::fs::remove_dir_all(opts.dir_path.clone()).unwrap();

        engine
            .fold(|key, value| {
                println!("{:?}", key);
                println!("{:?}", value);

                true
            })
            .unwrap();
    }
    #[test]
    fn test_iterator_seek() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("./tmp/bitcask-rs-iter-seek");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let mut iter1 = engine.iter(IteratorOptions::default());
        iter1.seek("1".as_bytes().to_vec());
        // println!("{:?}",iter1.next());
        assert!(iter1.next().is_none());

        let put_res1 = engine.put(
            Bytes::from("1".as_bytes().to_vec()),
            rand_kv::get_test_value(1),
        );
        assert!(put_res1.is_ok());
        let mut iter2 = engine.iter(IteratorOptions::default());
        iter2.seek("1".as_bytes().to_vec());
        // assert!(iter2.next().is_some());
        println!("{:?}", iter2.next());

        let put_res2 = engine.put(
            Bytes::from("2".as_bytes().to_vec()),
            rand_kv::get_test_value(2),
        );
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(
            Bytes::from("3".as_bytes().to_vec()),
            rand_kv::get_test_value(3),
        );
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(
            Bytes::from("4".as_bytes().to_vec()),
            rand_kv::get_test_value(4),
        );
        assert!(put_res4.is_ok());

        let mut iter3 = engine.iter(IteratorOptions::default());
        iter3.seek("1".as_bytes().to_vec());

        while let Some(item) = iter3.next() {
            println!("{:?}", item);
            assert!(item.0.len() > 0);
        }

        println!("{:?}", engine.list_keys());
        std::fs::remove_dir_all(opts.dir_path.clone()).unwrap();
    }
}
