use std::{error, result};
use thiserror::Error;
#[derive(Error, Debug)]
pub enum Errors {
    #[error("failed to read from data file")]
    FailedToReadFromDataFile,

    #[error("failed to write to data file")]
    FailedToWriteToDataFile,

    #[error("failed to sync data file")]
    FailedToSyncDataFile,

    #[error("failed to open data file")]
    FailedToOpenDataFile,

    #[error("key is empty")]
    KeyIsEmpty,

    #[error("key not found")]
    KeyNotFound,

    #[error("index update failed")]
    IndexUpdateFailed,
    
    #[error("data file not found")]
    DataFileNotFound,
    
    #[error("dir path is empty")]
    DirPathIsEmpty,

    #[error("dir file size too small")]
    DirFileSizeTooSmall,
    #[error("failed to create database dir")]
    FailedToCreateDataBaseDir,
    #[error("failed to read database dir")]
    FailedToReadDataBaseDir,
}
pub type Result<T> = result::Result<T, Errors>;
