use std::{path::PathBuf, sync::Arc};

use crate::{
    data::log_record::max_long_record,
    errors::{Errors, Result},
    fio::{self, new_io_manager},
};
use bytes::{Buf, BytesMut};
use parking_lot::RwLock;
use prost::{decode_length_delimiter, length_delimiter_len};

use super::log_record::{LogRecord, LogRecordType, ReadLogRecord};
pub const DATA_FILE_NAME_SUFFIX: &str = ".data";
pub struct DataFile {
    file_id: Arc<RwLock<u32>>,
    write_off: Arc<RwLock<u64>>,
    io_manager: Box<dyn fio::IOManager>,
}
impl DataFile {
    pub fn new(dir_path: PathBuf, file_id: u32) -> Result<DataFile> {
        let file_name = get_data_file_name(dir_path, file_id);
        let io_manager = new_io_manager(file_name)?;
        Ok(DataFile {
            file_id: Arc::new(RwLock::new(file_id)),
            write_off: Arc::new(RwLock::new(0)),
            io_manager: Box::new(io_manager),
        })
    }
    pub fn get_write_off(&self) -> u64 {
        let read_guard = self.write_off.read();
        *read_guard
    }
    pub fn set_write_offset(&self, offset: u64) {
        let mut write_guard = self.write_off.write();
        *write_guard = offset;
    }
    pub fn get_file_id(&self) -> u32 {
        let read_guard = self.file_id.read();
        *read_guard
    }
    pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
        let mut header_buf = BytesMut::zeroed(max_long_record());

        self.io_manager.read(&mut header_buf, offset)?;

        let rec_type = header_buf.get_u8();

        let key_size = decode_length_delimiter(&mut header_buf).unwrap();
        let value_size = decode_length_delimiter(&mut header_buf).unwrap();

        if key_size == 0 && value_size == 0 {
            return Err(Errors::ReadDataFileEOF);
        }
        let actual_header_size =
            length_delimiter_len(key_size) + length_delimiter_len(value_size) + 1;
        let mut kv_buf = BytesMut::zeroed(key_size + value_size + 4);
        self.io_manager
            .read(&mut kv_buf, offset + actual_header_size as u64)?;
        let log_record = LogRecord {
            key: kv_buf.get(..key_size).unwrap().to_vec(),
            value: kv_buf.get(key_size..kv_buf.len() - 4).unwrap().to_vec(),
            rec_type: LogRecordType::from_u8(rec_type),
        };
        kv_buf.advance(key_size + value_size);
        if kv_buf.get_u32() != log_record.get_crc() {
            return Err(Errors::InvalidLogRecordCrc);
        }
        Ok(ReadLogRecord {
            record: log_record,
            size: actual_header_size + key_size + value_size + 4,
        })
    }
    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        let n_bytes = self.io_manager.write(buf)?;
        let mut write_off = self.write_off.write();
        *write_off += n_bytes as u64;
        Ok(n_bytes)
    }
    pub fn sync(&self) -> Result<()> {
        self.io_manager.sync()
    }
}
fn get_data_file_name(dir_path: PathBuf, file_id: u32) -> PathBuf {
    let name = std::format!("{:09}", file_id) + DATA_FILE_NAME_SUFFIX;
    dir_path.join(name)
}

#[cfg(test)]
mod tests {

    use super::*;
    #[test]
    fn test_new_data_file() {
        let dir_path = std::env::temp_dir();

        let data_file_res1 = DataFile::new(dir_path.clone(), 0);
        assert!(data_file_res1.is_ok());
        let data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 0);
        println!("temp dir:{:?}", dir_path.clone().as_os_str());

        let data_file_res2 = DataFile::new(dir_path.clone(), 0);
        assert!(data_file_res2.is_ok());
        let data_file2 = data_file_res2.unwrap();
        assert_eq!(data_file2.get_file_id(), 0);
        println!("temp dir:{:?}", dir_path.clone().as_os_str());

        let data_file_res3 = DataFile::new(dir_path.clone(), 660);
        assert!(data_file_res3.is_ok());
        let data_file3 = data_file_res3.unwrap();
        assert_eq!(data_file3.get_file_id(), 660);
        println!("temp dir:{:?}", dir_path.clone().as_os_str());
    }
    #[test]
    fn test_data_file_write() {
        let dir_path = std::env::temp_dir();

        let data_file_res1 = DataFile::new(dir_path.clone(), 10);
        assert!(data_file_res1.is_ok());
        let data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 10);
        println!("temp dir:{:?}", dir_path.clone().as_os_str());

        let write_res1 = data_file1.write("sang".as_bytes());
        assert!(write_res1.is_ok());
        assert_eq!(write_res1.unwrap(), 4);

        let write_res2 = data_file1.write("xia".as_bytes());
        assert!(write_res2.is_ok());
        assert_eq!(write_res2.unwrap(), 3);
    }

    #[test]
    fn test_data_file_sync() {
        let dir_path = std::env::temp_dir();

        let data_file_res1 = DataFile::new(dir_path.clone(), 100);
        assert!(data_file_res1.is_ok());
        let data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 100);
        println!("temp dir:{:?}", dir_path.clone().as_os_str());

        let sync_res1 = data_file1.sync();
        assert!(sync_res1.is_ok());
    }
    #[test]
    fn test_data_file_read_log_record() {
        let dir_path = std::env::temp_dir();

        let data_file_res1 = DataFile::new(dir_path.clone(), 200);
        assert!(data_file_res1.is_ok());
        let data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 200);

        let enc1 = LogRecord {
            key: "xia".as_bytes().to_vec(),
            value: "sang".as_bytes().to_vec(),
            rec_type: LogRecordType::NORMAL,
        };

        let write_enc1 = data_file1.write(&enc1.encode());
        assert!(write_enc1.is_ok());

        let read_enc1 = data_file1.read_log_record(0);
        assert!(read_enc1.is_ok());
        println!("enc1: {:?}", read_enc1);
        let record = read_enc1.ok().unwrap();
        let read_enc1 = record.record;
        assert_eq!(read_enc1.key, enc1.key);
        assert_eq!(read_enc1.value, enc1.value);
        assert_eq!(read_enc1.rec_type, enc1.rec_type);

        let enc2 = LogRecord {
            key: "sang".as_bytes().to_vec(),
            value: "xia".as_bytes().to_vec(),
            rec_type: LogRecordType::DElETED,
        };

        let write_enc2 = data_file1.write(&enc2.encode());
        assert!(write_enc2.is_ok());

        let read_enc2 = data_file1.read_log_record(14);
        println!("enc2: {:?}", read_enc2);
        let read_enc2 = read_enc2.ok().unwrap().record;
        assert_eq!(read_enc2.key, enc2.key);
        assert_eq!(read_enc2.value, enc2.value);
        assert_eq!(read_enc2.rec_type, enc2.rec_type);
    }
}
