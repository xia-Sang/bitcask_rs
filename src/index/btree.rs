use std::{collections::BTreeMap, sync::Arc};

use crate::data::log_record::LogRecordPos;
use parking_lot::RwLock;

use super::Indexer;

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
}
