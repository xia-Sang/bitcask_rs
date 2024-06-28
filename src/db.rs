use std::{collections::HashMap, sync::Arc};

use crate::{
    data::{
        data_file::DataFile,
        log_record::{LogRecord, LogRecordPos, LogRecordType},
    },
    errors::{Errors, Result},
    index,
    options::Options,
};
use bytes::Bytes;
use parking_lot::RwLock;

pub struct Engine {
    options: Arc<Options>,
    active_file: Arc<RwLock<DataFile>>,
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    index: Box<dyn index::Indexer>,
}
impl Engine {
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        let mut record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: LogRecordType::NORMAL,
        };
        let log_record = self.append_log_record(&mut record)?;
        match self.index.put(key.to_vec(), log_record) {
            false => Err(Errors::IndexUpdateFailed),
            true => Ok(()),
        }
    }
    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Err(Errors::KeyIsEmpty);
        }
        let log_record_pos = pos.unwrap();
        let active_file = self.active_file.read();
        let older_files = self.older_files.read();
        let log_record = match active_file.get_file_id() == log_record_pos.file_id {
            true => active_file.read_log_record(log_record_pos.offset)?,
            false => {
                let data_file = older_files.get(&log_record_pos.file_id);
                if data_file.is_none() {
                    return Err(Errors::DataFileNotFound);
                }
                data_file.unwrap().read_log_record(log_record_pos.offset)?
            }
        };

        if log_record.rec_type == LogRecordType::DElETED {
            return Err(Errors::KeyNotFound);
        }
        Ok(log_record.value.into())
    }
    fn append_log_record(&self, log_record: &mut LogRecord) -> Result<LogRecordPos> {
        let dir_path = self.options.dir_path.clone();
        let enc_record = log_record.encode();
        let record_len = enc_record.len() as u64;

        let mut active_file = self.active_file.write();

        if active_file.get_write_off() + record_len > self.options.data_file_size {
            active_file.sync()?;

            let current_fid = active_file.get_file_id();
            let mut older_files = self.older_files.write();
            let old_file = DataFile::new(dir_path.clone(), current_fid)?;
            older_files.insert(current_fid, old_file);

            let new_file = DataFile::new(dir_path.clone(), current_fid + 1)?;
            *active_file = new_file;
        }
        let write_off = active_file.get_write_off();
        active_file.write(&enc_record)?;
        if self.options.sync_writes {
            active_file.sync()?;
        }

        Ok(LogRecordPos {
            file_id: active_file.get_file_id(),
            offset: write_off,
        })
    }
}
