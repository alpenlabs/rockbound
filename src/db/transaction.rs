use crate::{metrics::{SCHEMADB_GET_BYTES, SCHEMADB_GET_LATENCY_SECONDS}, schema::{KeyCodec, ValueCodec}, CommonDB, OptimisticTransactionDB, Schema, TransactionDB};


pub trait TransactionDBMarker: CommonDB {}
impl TransactionDBMarker for TransactionDB {}
impl TransactionDBMarker for OptimisticTransactionDB {}


/// operations inside transaction
pub struct TransactionCtx<'db, DB: TransactionDBMarker> {
    pub(crate) txn: &'db rocksdb::Transaction<'db, DB::DB>,
    pub(crate) db: &'db DB,
}

impl<'db, DB: TransactionDBMarker> TransactionCtx<'db, DB> {
    /// Get underlying transaction
    pub fn txn(&self) -> &'db rocksdb::Transaction<'db, DB::DB> {
        self.txn
    }

    /// Reads single record by key.
    pub fn get<S: Schema>(
        &self,
        schema_key: &impl KeyCodec<S>,
    ) -> anyhow::Result<Option<S::Value>> {
        let _timer = SCHEMADB_GET_LATENCY_SECONDS
            .with_label_values(&[S::COLUMN_FAMILY_NAME])
            .start_timer();

        let k = schema_key.encode_key()?;
        let cf_handle = self.db.get_cf_handle(S::COLUMN_FAMILY_NAME)?;

        let result = self.txn.get_pinned_cf(cf_handle, k)?;
        SCHEMADB_GET_BYTES
            .with_label_values(&[S::COLUMN_FAMILY_NAME])
            .observe(result.as_ref().map_or(0.0, |v| v.len() as f64));

        result
            .map(|raw_value| <S::Value as ValueCodec<S>>::decode_value(&raw_value))
            .transpose()
            .map_err(|err| err.into())
    }

    /// Writes single record.
    pub fn put<S: Schema>(
        &self,
        key: &impl KeyCodec<S>,
        value: &impl ValueCodec<S>,
    ) -> anyhow::Result<()> {
        // Not necessary to use a batch, but we'd like a central place to bump counters.
        // Used in tests only anyway.
        let cf_handle = self.db.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        self.txn
            .put_cf(cf_handle, key.encode_key()?, value.encode_value()?)?;

        Ok(())
    }

    /// Delete a single key from the database.
    pub fn delete<S: Schema>(&self, key: &impl KeyCodec<S>) -> anyhow::Result<()> {
        // Not necessary to use a batch, but we'd like a central place to bump counters.
        // Used in tests only anyway.
        let cf_handle = self.db.get_cf_handle(S::COLUMN_FAMILY_NAME)?;

        self.txn.delete_cf(cf_handle, key.encode_key()?)?;

        Ok(())
    }
}


/// error return for transaction
#[derive(Debug, PartialEq)]
pub enum TransactionError<Reason: Into<anyhow::Error>> {
    /// custom error specified on call
    Rollback(Reason),
    /// max retries exceeded
    MaxRetriesExceeded,
    /// other rocksdb related error
    ErrorKind(rocksdb::ErrorKind),
}

impl<T: Into<anyhow::Error>> From<TransactionError<T>> for anyhow::Error {
    fn from(value: TransactionError<T>) -> Self {
        match value {
            TransactionError::Rollback(err) => err.into(),
            TransactionError::MaxRetriesExceeded => {
                anyhow::Error::msg("TransactionError: Max retries exceeded")
            }
            TransactionError::ErrorKind(error_kind) => anyhow::Error::msg(match error_kind {
                rocksdb::ErrorKind::NotFound => "NotFound",
                rocksdb::ErrorKind::Corruption => "Corruption",
                rocksdb::ErrorKind::NotSupported => "Not implemented",
                rocksdb::ErrorKind::InvalidArgument => "Invalid argument",
                rocksdb::ErrorKind::IOError => "IO error",
                rocksdb::ErrorKind::MergeInProgress => "Merge in progress",
                rocksdb::ErrorKind::Incomplete => "Result incomplete",
                rocksdb::ErrorKind::ShutdownInProgress => "Shutdown in progress",
                rocksdb::ErrorKind::TimedOut => "Operation timed out",
                rocksdb::ErrorKind::Aborted => "Operation aborted",
                rocksdb::ErrorKind::Busy => "Resource busy",
                rocksdb::ErrorKind::Expired => "Operation expired",
                rocksdb::ErrorKind::TryAgain => "Operation failed. Try again.",
                rocksdb::ErrorKind::CompactionTooLarge => "Compaction too large",
                rocksdb::ErrorKind::ColumnFamilyDropped => "Column family dropped",
                rocksdb::ErrorKind::Unknown => "Unknown",
            }),
        }
    }
}
