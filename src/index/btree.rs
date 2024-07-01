use std::{collections::BTreeMap, sync::Arc};

use crate::{data::log_record::LogRecordPos, errors::Result, options::IteratorOptions};
use bytes::Bytes;
use parking_lot::RwLock;

use super::{IndexIterator, Indexer};

pub struct Btree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}
impl Btree {
    pub fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}
impl Indexer for Btree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let mut write_guard = self.tree.write();
        write_guard.insert(key, pos);
        true
    }
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let read_guard = self.tree.read();
        read_guard.get(&key).copied()
    }
    fn delete(&self, key: Vec<u8>) -> bool {
        let mut write_guard = self.tree.write();
        let remove_res = write_guard.remove(&key);
        remove_res.is_some()
    }
    fn list_keys(&self) -> Result<Vec<Bytes>> {
        let read_guard = self.tree.read();
        let mut keys = Vec::with_capacity(read_guard.len());
        for (k, _) in read_guard.iter() {
            keys.push(Bytes::copy_from_slice(&k));
        }
        Ok(keys)
    }
    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator> {
        let read_guard = self.tree.read();
        let mut items = Vec::with_capacity(read_guard.len());

        for (key, val) in read_guard.iter() {
            items.push((key.clone(), val.clone()));
        }
        if options.reverse {
            items.reverse();
        }
        Box::new(BtreeIterator {
            items,
            curr_index: 0,
            options,
        })
    }
}
pub struct BtreeIterator {
    items: Vec<(Vec<u8>, LogRecordPos)>,
    curr_index: usize,
    options: IteratorOptions,
}
impl IndexIterator for BtreeIterator {
    fn rewind(&mut self) {
        self.curr_index = 0;
    }

    fn seek(&mut self, key: Vec<u8>) {
        self.curr_index = match self.items.binary_search_by(|(x, _)| {
            if self.options.reverse {
                x.cmp(&key).reverse()
            } else {
                x.cmp(&key)
            }
        }) {
            Ok(equal_val) => equal_val,
            Err(insert_val) => insert_val,
        };
    }

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)> {
        if self.curr_index >= self.items.len() {
            return None;
        }
        while let Some(item) = self.items.get(self.curr_index) {
            self.curr_index += 1;
            let prefix = &self.options.prefix;
            if prefix.is_empty() || item.0.starts_with(&prefix) {
                return Some((&item.0, &item.1));
            }
        }
        None
    }
}
#[cfg(test)]
mod tests {

    use super::*;
    #[test]
    fn test_btree_put() {
        let bt = Btree::new();
        let res1 = bt.put(
            "gsegseg".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        assert_eq!(res1, true);

        let res2 = bt.put(
            "fsefsegs".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 2,
                offset: 10,
            },
        );
        assert_eq!(res2, true);
    }
    #[test]
    fn test_btree_get() {
        let bt = Btree::new();
        let res1 = bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        assert_eq!(res1, true);

        let res2 = bt.put(
            "sang".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 2,
                offset: 10,
            },
        );
        assert_eq!(res2, true);

        let pos1 = bt.get("".as_bytes().to_vec());
        assert!(pos1.is_some());
        assert_eq!(pos1.unwrap().file_id, 1);
        assert_eq!(pos1.unwrap().offset, 10);

        let pos2 = bt.get("sang".as_bytes().to_vec());
        assert!(pos2.is_some());
        assert_eq!(pos2.unwrap().file_id, 2);
        assert_eq!(pos2.unwrap().offset, 10);
    }
    #[test]
    fn test_btree_del() {
        let bt = Btree::new();
        let res1 = bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        assert_eq!(res1, true);

        let res2 = bt.put(
            "sang".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 2,
                offset: 10,
            },
        );
        assert_eq!(res2, true);

        let del1 = bt.delete("".as_bytes().to_vec());
        assert!(del1);

        let del2 = bt.delete("sang".as_bytes().to_vec());
        assert!(del2);

        let del3 = bt.delete("data not exist".as_bytes().to_vec());
        assert!(!del3);
    }
    #[test]
    fn test_btree_iterator_seek() {
        let bt = Btree::new();

        // 没有数据时候
        let mut iter1 = bt.iterator(IteratorOptions::default());
        iter1.seek("1".as_bytes().to_vec());
        let res1 = iter1.next();
        assert!(res1.is_none());
        bt.put(
            "1".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        // 拥有一条数据的时候
        // 没有数据时候
        let mut iter2 = bt.iterator(IteratorOptions::default());
        iter2.seek("1".as_bytes().to_vec());
        let res2 = iter2.next();
        println!("{:?}", res2);
        assert!(res2.is_some());

        let mut iter3 = bt.iterator(IteratorOptions::default());
        iter3.seek("2".as_bytes().to_vec());
        let res3 = iter3.next();
        println!("{:?}", res3);
        assert!(res3.is_none());

        bt.put(
            "2".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "3".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "4".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "5".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );

        let mut iter4 = bt.iterator(IteratorOptions::default());
        iter4.seek("1".as_bytes().to_vec());
        while let Some(item) = iter4.next() {
            println!("{:?}", String::from_utf8(item.0.to_vec()));
            assert!(item.0.len() > 0);
        }

        let mut iter5 = bt.iterator(IteratorOptions::default());
        iter5.seek("6".as_bytes().to_vec());
        let res5 = iter5.next();
        println!("{:?}", res5);
        assert!(res5.is_none());
    }
}
