use bytes::{BufMut, BytesMut};
use prost::{encode_length_delimiter, length_delimiter_len};

#[derive(PartialEq, Clone, Copy, Debug)]
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
#[derive(Debug)]
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
#[derive(Debug)]
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: usize,
}
impl LogRecord {
    // 进行数据encode操作
    pub fn encode(&self) -> Vec<u8> {
        let (enc_buf, _) = self.encode_and_get_crc();
        enc_buf
    }
    pub fn get_crc(&self) -> u32 {
        let (_, crc_value) = self.encode_and_get_crc();
        crc_value
    }
    // type keySize valueSize key value crc
    fn encode_and_get_crc(&self) -> (Vec<u8>, u32) {
        let mut buf = BytesMut::new();
        buf.reserve(self.encoded_length());

        // type
        buf.put_u8(self.rec_type as u8);

        // key and value size
        encode_length_delimiter(self.key.len(), &mut buf).unwrap();
        encode_length_delimiter(self.value.len(), &mut buf).unwrap();

        // key and value
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        // cal crc
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        buf.put_u32(crc);

        // println!("{}",crc);
        (buf.to_vec(), crc)
    }
    // 计算数据长度
    fn encoded_length(&self) -> usize {
        std::mem::size_of::<u8>()
            + length_delimiter_len(self.key.len())
            + length_delimiter_len(self.key.len())
            + self.key.len()
            + self.value.len()
            + 4
    }
}

pub fn max_long_record() -> usize {
    // type keySize valueSize key value crc
    std::mem::size_of::<u8>() + length_delimiter_len(std::u32::MAX as usize) * 2
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_log_record_encode_and_crc() {
        // 进行正常的测试
        let rec1 = LogRecord {
            key: "xia".as_bytes().to_vec(),
            value: "sang".as_bytes().to_vec(),
            rec_type: LogRecordType::NORMAL,
        };
        let enc1 = rec1.encode();
        // println!("{:?}",enc1);
        // println!("{}",rec1.get_crc());
        assert!(enc1.len() > 5);
        assert_eq!(4254512099, rec1.get_crc());

        // value 为空
        let rec2 = LogRecord {
            key: "xia".as_bytes().to_vec(),
            value: Default::default(),
            rec_type: LogRecordType::NORMAL,
        };
        let enc2 = rec2.encode();
        // println!("{:?}",enc1);
        println!("{}", rec2.get_crc());
        assert!(enc2.len() > 5);
        assert_eq!(3914772510, rec2.get_crc());

        // type= delete
        let rec3 = LogRecord {
            key: "xia".as_bytes().to_vec(),
            value: "sang".as_bytes().to_vec(),
            rec_type: LogRecordType::DElETED,
        };
        let enc3 = rec3.encode();
        // println!("{:?}",enc1);
        println!("{}", rec3.get_crc());
        assert!(enc3.len() > 5);
        assert_eq!(379652320, rec3.get_crc());
    }
}
