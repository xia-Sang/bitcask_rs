<<<<<<< Updated upstream
#[derive(Clone,Copy,Debug)]
pub struct LogRecordPos{
    pub(crate)file_id:u32,
    pub(crate)offset:u64,
    // 这是一段测试代码
}
=======
#[derive(PartialEq)]
pub enum LogRecordType {
    NORMAL = 1,
    DElETED = 2,
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

impl LogRecord {
    pub fn encode(&mut self) -> Vec<u8> {
        todo!()
    }
}
>>>>>>> Stashed changes
