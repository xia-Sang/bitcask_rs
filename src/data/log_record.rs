use prost::length_delimiter_len;

#[derive(PartialEq)]
pub enum LogRecordType {
    NORMAL = 1,
    DElETED = 2,
}
impl LogRecordType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => LogRecordType::NORMAL,
            2 => LogRecordType::DElETED,
            _ => panic!("unknown log record type"),
        }
    }
}
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

#[derive(Clone, Copy, Debug)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: usize,
}
impl LogRecord {
    pub fn encode(&mut self) -> Vec<u8> {
        todo!()
    }
    pub fn get_crc(&mut self) -> u32 {
        todo!()
    }
}

pub fn max_long_record() -> usize {
    // type keySize valueSize key value crc
    std::mem::size_of::<u8>() + length_delimiter_len(std::u32::MAX as usize) * 2
}
