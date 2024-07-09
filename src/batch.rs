use crate::{
    data::log_record::{LogRecord, LogRecordType},
    db::Engine,
    errors::{Errors, Result},
    options::WriteBatchOptions,
};
use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use prost::{decode_length_delimiter, encode_length_delimiter};
use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc},
};

const TXN_FIN_KEY: &[u8] = "txn_finish".as_bytes();
pub(crate) const NON_TRANSACTION_SEQ_NO: usize = 0;
/// 实现批量写操作
pub struct WriteBatch<'a> {
    pending_writes: Arc<Mutex<HashMap<Vec<u8>, LogRecord>>>,
    engine: &'a Engine,
    options: WriteBatchOptions,
}
impl Engine {
    /// 初始化
    pub fn new_write_batch(&self, options: WriteBatchOptions) -> Result<WriteBatch> {
        Ok(WriteBatch {
            pending_writes: Arc::new(Mutex::new(HashMap::new())),
            engine: self,
            options,
        })
    }
}
impl WriteBatch<'_> {
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        let log_record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: LogRecordType::NORMAL,
        };
        let mut pending_writes = self.pending_writes.lock();
        pending_writes.insert(key.to_vec(), log_record);
        Ok(())
    }
    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        let mut pending_writes = self.pending_writes.lock();
        let index_pos = self.engine.index.get(key.to_vec());
        if index_pos.is_none() {
            if pending_writes.contains_key(&key.to_vec()) {
                pending_writes.remove(&key.to_vec());
            }
        }
        let log_record = LogRecord {
            key: key.to_vec(),
            value: Default::default(),
            rec_type: LogRecordType::DElETED,
        };
        pending_writes.insert(key.to_vec(), log_record);
        Ok(())
    }
    pub fn commit(&self) -> Result<()> {
        let mut pending_writes = self.pending_writes.lock();
        if pending_writes.len() == 0 {
            return Ok(());
        }
        if pending_writes.len() > self.options.max_batch_num {
            return Err(Errors::ExceedMaxBatchNum);
        }

        let _lock = self.engine.batch_commit_lock.lock();

        let seq_no = self.engine.seq_no.fetch_add(1, Ordering::SeqCst);

        let mut positions = HashMap::new();
        for (_, item) in pending_writes.iter() {
            let mut log_record = LogRecord {
                key: log_record_key_with_seq(item.key.clone(), seq_no),
                value: item.value.clone(),
                rec_type: item.rec_type,
            };
            let pos = self.engine.append_log_record(&mut log_record)?;
            positions.insert(item.key.clone(), pos);
        }
        let mut finish_record = LogRecord {
            key: log_record_key_with_seq(TXN_FIN_KEY.to_vec(), seq_no),
            value: Default::default(),
            rec_type: LogRecordType::TxnFinish,
        };
        self.engine.append_log_record(&mut finish_record)?;

        // 数据持久化
        if self.options.sync_writes {
            self.engine.sync()?;
        }

        for (_, item) in pending_writes.iter() {
            let record_pos = positions.get(&item.key).unwrap();
            if item.rec_type == LogRecordType::NORMAL {
                self.engine.index.put(item.key.clone(), *record_pos);
            }
            if item.rec_type == LogRecordType::DElETED {
                self.engine.index.delete(item.key.clone());
            }
        }
        pending_writes.clear();
        Ok(())
    }
}

pub(crate) fn log_record_key_with_seq(key: Vec<u8>, seq_no: usize) -> Vec<u8> {
    let mut enc_key = BytesMut::new();
    encode_length_delimiter(seq_no, &mut enc_key).unwrap();
    enc_key.extend_from_slice(&key.to_vec());
    enc_key.to_vec()
}
pub(crate) fn parse_log_record_key(key: Vec<u8>) -> (Vec<u8>, usize) {
    let mut buf = BytesMut::new();
    buf.put_slice(&key);
    let seq_no = decode_length_delimiter(&mut buf).unwrap();

    (buf.to_vec(), seq_no)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{options::Options, util::rand_kv::*};

    use super::*;
    #[test]
    fn test_write_batch_1() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("./tmp/bitcask-rs-batch-1");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let wb = engine
            .new_write_batch(WriteBatchOptions::default())
            .unwrap();
        let put_res1 = wb.put(get_test_key(1), get_test_value(12));
        assert!(put_res1.is_ok());
        let put_res2 = wb.put(get_test_key(2), get_test_value(12));
        assert!(put_res2.is_ok());

        let res0 = engine.get(get_test_key(1));
        // println!("{:?}",res0);
        assert_eq!(Errors::KeyNotFound, res0.err().unwrap());

        let commit_res = wb.commit();
        assert!(commit_res.is_ok());
        let res1 = engine.get(get_test_key(1));
        println!("{:?}", res1);

        let seq_no = wb.engine.seq_no.load(Ordering::SeqCst);
        // println!("{}", seq_no);
        assert_eq!(2, seq_no);
        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }
    #[test]
    fn test_write_batch_2() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("./tmp/bitcask-rs-batch-2");
        opts.data_file_size = 64 * 1024 * 1024;

        let engine0 = Engine::open(opts.clone()).expect("failed to open engine");

        let wb = engine0
            .new_write_batch(WriteBatchOptions::default())
            .unwrap();
        let put_res1 = wb.put(get_test_key(1), get_test_value(12));
        assert!(put_res1.is_ok());
        let put_res2 = wb.put(get_test_key(2), get_test_value(12));
        assert!(put_res2.is_ok());
        let commit_res = wb.commit();
        assert!(commit_res.is_ok());
        let seq_no_0 = engine0.seq_no.load(Ordering::SeqCst);
        assert_eq!(2,seq_no_0);
        engine0.close().expect("close error");

        let engine1 = Engine::open(opts.clone()).expect("open file failedd");
        println!("{:#?}", engine1.list_keys());

        let seq_no_1 = engine1.seq_no.load(Ordering::SeqCst);
        // println!("{}", seq_no_1);
        assert_eq!(2, seq_no_1);

        let put_res1 = wb.put(get_test_key(3), get_test_value(12));
        assert!(put_res1.is_ok());
        let put_res2 = wb.put(get_test_key(4), get_test_value(12));
        assert!(put_res2.is_ok());
        let commit_res = wb.commit();
        assert!(commit_res.is_ok());

        engine1.close().expect("close error");

        let engine2 = Engine::open(opts.clone()).expect("open file failedd");
        println!("{:#?}", engine2.list_keys());
        let seq_no = engine2.seq_no.load(Ordering::SeqCst);
        println!("{}", seq_no);
        assert_eq!(3, seq_no);
        println!("{:#?}", engine2.list_keys());

        engine2.close().expect("close error");
        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }
}
