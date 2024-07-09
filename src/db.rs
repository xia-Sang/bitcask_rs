use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    u32,
};

use crate::{
    batch::{log_record_key_with_seq, parse_log_record_key, NON_TRANSACTION_SEQ_NO},
    data::{
        data_file::{DataFile, DATA_FILE_NAME_SUFFIX},
        log_record::{LogRecord, LogRecordPos, LogRecordType, TransactionRecord},
    },
    errors::{Errors, Result},
    index,
    options::Options,
};
use bytes::Bytes;
use log::warn;
use parking_lot::{Mutex, RwLock};

const INITIAL_FILE_ID: u32 = 0;
pub struct Engine {
    options: Arc<Options>,
    active_file: Arc<RwLock<DataFile>>,
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    pub(crate) index: Box<dyn index::Indexer>,
    file_ids: Vec<u32>,
    pub(crate) batch_commit_lock: Mutex<()>,
    pub(crate) seq_no: Arc<AtomicUsize>,
}
impl Engine {
    pub fn open(opts: Options) -> Result<Self> {
        if let Some(e) = check_options(&opts) {
            return Err(e);
        }
        let options = opts.clone();

        let dir_path = options.dir_path.clone();
        if !dir_path.is_dir() {
            if let Err(e) = fs::create_dir_all(dir_path.clone()) {
                warn!("create database directory err:{}", e);
                return Err(Errors::FailedToCreateDataBaseDir);
            }
        }
        let mut data_files = load_data_files(dir_path.clone())?;

        let mut file_ids = Vec::new();
        for v in data_files.iter() {
            file_ids.push(v.get_file_id());
        }

        data_files.reverse();

        let mut older_files = HashMap::new();
        if data_files.len() > 1 {
            for _ in 0..=data_files.len() - 2 {
                let file = data_files.pop().unwrap();
                older_files.insert(file.get_file_id(), file);
            }
        }
        let active_file = match data_files.pop() {
            Some(v) => v,
            None => DataFile::new(dir_path.clone(), INITIAL_FILE_ID)?,
        };
        let mut engine = Self {
            options: Arc::new(opts),
            active_file: Arc::new(RwLock::new(active_file)),
            older_files: Arc::new(RwLock::new(older_files)),
            index: Box::new(index::new_index(options.index_type)),
            file_ids: file_ids,
            batch_commit_lock: Mutex::new(()),
            seq_no: Arc::new(AtomicUsize::new(1)),
        };
        let current_seq_no = engine.load_index_from_data_files()?;
        if current_seq_no > 0 {
            engine.seq_no.store(current_seq_no+1, Ordering::SeqCst);
        }
        Ok(engine)
    }
    pub fn close(&self) -> Result<()> {
        let read_guard = self.active_file.read();
        read_guard.sync()
    }
    pub fn sync(&self) -> Result<()> {
        let read_guard = self.active_file.read();
        read_guard.sync()
    }
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        let mut record = LogRecord {
            key: log_record_key_with_seq(key.to_vec(), NON_TRANSACTION_SEQ_NO),
            value: value.to_vec(),
            rec_type: LogRecordType::NORMAL,
        };

        let log_record_pos = self.append_log_record(&mut record)?;
        match self.index.put(key.to_vec(), log_record_pos) {
            false => Err(Errors::IndexUpdateFailed),
            true => Ok(()),
        }
    }
    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Ok(());
        }
        let mut record = LogRecord {
            key: log_record_key_with_seq(key.to_vec(), NON_TRANSACTION_SEQ_NO),
            value: Default::default(),
            rec_type: LogRecordType::DElETED,
        };
        self.append_log_record(&mut record)?;

        let ok = self.index.delete(key.to_vec());
        if !ok {
            return Err(Errors::IndexUpdateFailed);
        }
        Ok(())
    }
    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        // println!("key: {:?}",key);
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Err(Errors::KeyNotFound);
        }
        let log_record_pos = pos.unwrap();
        self.get_value_by_position(&log_record_pos)
    }
    pub(crate) fn get_value_by_position(&self, log_record_pos: &LogRecordPos) -> Result<Bytes> {
        let active_file = self.active_file.read();
        let older_files = self.older_files.read();
        let log_record = match active_file.get_file_id() == log_record_pos.file_id {
            true => active_file.read_log_record(log_record_pos.offset)?.record,
            false => {
                let data_file = older_files.get(&log_record_pos.file_id);
                if data_file.is_none() {
                    return Err(Errors::DataFileNotFound);
                }
                data_file
                    .unwrap()
                    .read_log_record(log_record_pos.offset)?
                    .record
            }
        };

        if log_record.rec_type == LogRecordType::DElETED {
            return Err(Errors::KeyNotFound);
        }
        Ok(log_record.value.into())
    }
    pub(crate) fn append_log_record(&self, log_record: &mut LogRecord) -> Result<LogRecordPos> {
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
    fn load_index_from_data_files(&mut self) -> Result<usize> {
        let mut current_seq_no = NON_TRANSACTION_SEQ_NO;
        if self.file_ids.is_empty() {
            return Ok(current_seq_no);
        }

        let mut transaction_records = HashMap::new();

        let active_file = self.active_file.read();
        let older_files = self.older_files.read();

        for (i, file_id) in self.file_ids.iter().enumerate() {
            let mut offset = 0;
            loop {
                let log_record_res = match *file_id == active_file.get_file_id() {
                    true => active_file.read_log_record(offset),
                    false => {
                        let data_file = older_files.get(file_id).unwrap();
                        data_file.read_log_record(offset)
                    }
                };
                let (mut log_record, size) = match log_record_res {
                    Ok(result) => (result.record, result.size),
                    Err(e) => {
                        if e == Errors::ReadDataFileEOF {
                            break;
                        }
                        return Err(e);
                    }
                };
                let log_record_pos = LogRecordPos {
                    file_id: *file_id,
                    offset,
                };
                let (real_key, seq_no) = parse_log_record_key(log_record.key.clone());
                // 如果不是事务提交的话
                if seq_no == NON_TRANSACTION_SEQ_NO {
                    self.update_index(real_key, log_record.rec_type, log_record_pos)
                } else {
                    if log_record.rec_type == LogRecordType::TxnFinish {
                        let records: &Vec<TransactionRecord> =
                            transaction_records.get(&seq_no).unwrap();
                        for txn_record in records.iter() {
                            self.update_index(
                                txn_record.record.key.clone(),
                                txn_record.record.rec_type,
                                txn_record.pos,
                            )
                        }
                        transaction_records.remove(&seq_no);
                    } else {
                        log_record.key = real_key;
                        transaction_records
                            .entry(seq_no)
                            .or_insert(Vec::new())
                            .push(TransactionRecord {
                                record: log_record,
                                pos: log_record_pos,
                            })
                    }
                }
                if seq_no > current_seq_no {
                    current_seq_no=seq_no;
                }

                offset += size as u64;
            }
            if i == self.file_ids.len() - 1 {
                active_file.set_write_offset(offset);
            }
        }

        Ok(current_seq_no)
    }
    fn update_index(&self, key: Vec<u8>, rec_type: LogRecordType, pos: LogRecordPos) {
        if rec_type == LogRecordType::NORMAL {
            self.index.put(key.clone(), pos);
        }
        if rec_type == LogRecordType::DElETED {
            self.index.delete(key.clone());
        }

        // let ok = match log_record.rec_type {
        //     LogRecordType::DElETED => self.index.delete(log_record.key.to_vec()),
        //     LogRecordType::NORMAL => self.index.put(log_record.key.to_vec(), log_record_pos),
        // };
        // if !ok {
        //     return Err(Errors::IndexUpdateFailed);
        // }
    }
}
fn check_options(opts: &Options) -> Option<Errors> {
    let dir_path = opts.dir_path.to_str();
    if dir_path.is_none() || dir_path.unwrap().len() == 0 {
        return Some(Errors::DirPathIsEmpty);
    }
    if opts.data_file_size <= 0 {
        return Some(Errors::DirFileSizeTooSmall);
    }
    None
}

fn load_data_files(dir_path: PathBuf) -> Result<Vec<DataFile>> {
    let dir = fs::read_dir(dir_path.clone());
    if dir.is_err() {
        return Err(Errors::FailedToReadDataBaseDir);
    }
    let mut file_ids: Vec<u32> = Vec::new();
    let mut data_files: Vec<DataFile> = Vec::new();
    for file in dir.unwrap() {
        if let Ok(entry) = file {
            let file_os_str = entry.file_name();
            let file_name = file_os_str.to_str().unwrap();

            if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
                let split_names: Vec<&str> = file_name.split('.').collect();
                let file_id = match split_names[0].parse::<u32>() {
                    Ok(fid) => fid,
                    Err(_) => {
                        return Err(Errors::DataDirectoryCorrupted);
                    }
                };
                file_ids.push(file_id);
            }
        }
    }

    if file_ids.is_empty() {
        return Ok(data_files);
    }
    file_ids.sort();

    for file_id in file_ids.iter() {
        let data_file = DataFile::new(dir_path.clone(), *file_id)?;
        data_files.push(data_file);
    }
    Ok(data_files)
}
