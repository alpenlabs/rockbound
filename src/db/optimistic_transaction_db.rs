use std::path::Path;

use anyhow::format_err;
use tracing::info;

use crate::metrics::{SCHEMADB_GET_BYTES, SCHEMADB_GET_LATENCY_SECONDS};
use crate::schema::{KeyCodec, Schema, ValueCodec};

use super::{CommonDB, RocksDB};

#[allow(missing_docs)]
impl RocksDB for rocksdb::OptimisticTransactionDB {
    type WriteBatch = rocksdb::WriteBatchWithTransaction<true>;
    
    fn cf_handle(&self, name: &str) -> Option<&rocksdb::ColumnFamily> {
        <rocksdb::OptimisticTransactionDB>::cf_handle(&self, name)
    }

    fn get_pinned_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl rocksdb::AsColumnFamilyRef,
        key: K,
    ) -> Result<Option<rocksdb::DBPinnableSlice>, rocksdb::Error> {
        rocksdb::OptimisticTransactionDB::get_pinned_cf(&self, cf, key)
    }

    fn write_opt(
        &self,
        batch: Self::WriteBatch,
        writeopts: &rocksdb::WriteOptions,
    ) -> Result<(), rocksdb::Error> {
        rocksdb::OptimisticTransactionDB::write_opt(&self, batch, writeopts)
    }

    fn raw_iterator_cf_opt<'a: 'b, 'b>(
        &'a self,
        cf_handle: &impl rocksdb::AsColumnFamilyRef,
        readopts: rocksdb::ReadOptions,
    ) -> rocksdb::DBRawIteratorWithThreadMode<'b, Self> {
        rocksdb::OptimisticTransactionDB::raw_iterator_cf_opt(&self, cf_handle, readopts)
    }
}

#[derive(Debug)]
#[allow(missing_docs)]
pub struct OptimisticTransactionDB {
    name: &'static str,
    db: rocksdb::OptimisticTransactionDB,
}

impl CommonDB for OptimisticTransactionDB {
    type DB = rocksdb::OptimisticTransactionDB;

    fn db(&self) -> &Self::DB {
        &self.db
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl OptimisticTransactionDB {
    fn log_construct(name: &'static str, db: rocksdb::OptimisticTransactionDB) -> Self {
        info!(rocksdb_name = name, "Opened RocksDB");
        Self { name, db }
    }

    /// Opens a database backed by RocksDB, using the provided column family names and default
    /// column family options.
    pub fn open(
        path: impl AsRef<Path>,
        name: &'static str,
        column_families: impl IntoIterator<Item = impl Into<String>>,
        db_opts: &rocksdb::Options,
    ) -> anyhow::Result<Self> {
        let db = Self::open_with_cfds(
            db_opts,
            path,
            name,
            column_families.into_iter().map(|cf_name| {
                let mut cf_opts = rocksdb::Options::default();
                cf_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
                rocksdb::ColumnFamilyDescriptor::new(cf_name, cf_opts)
            }),
        )?;
        Ok(db)
    }

    /// Open RocksDB with the provided column family descriptors.
    /// This allows to configure options for each column family.
    pub fn open_with_cfds(
        db_opts: &rocksdb::Options,
        path: impl AsRef<Path>,
        name: &'static str,
        cfds: impl IntoIterator<Item = rocksdb::ColumnFamilyDescriptor>,
    ) -> anyhow::Result<Self> {
        let db = rocksdb::OptimisticTransactionDB::open_cf_descriptors(db_opts, path, cfds)?;
        Ok(Self::log_construct(name, db))
    }

    /// Flushes [MemTable](https://github.com/facebook/rocksdb/wiki/MemTable) data.
    /// This is only used for testing `get_approximate_sizes_cf` in unit tests.
    pub fn flush_cf(&self, cf_name: &str) -> anyhow::Result<()> {
        Ok(self.db().flush_cf(self.get_cf_handle(cf_name)?)?)
    }

    /// Returns the current RocksDB property value for the provided column family name
    /// and property name.
    pub fn get_property(&self, cf_name: &str, property_name: &str) -> anyhow::Result<u64> {
        self.db()
            .property_int_value_cf(self.get_cf_handle(cf_name)?, property_name)?
            .ok_or_else(|| {
                format_err!(
                    "Unable to get property \"{}\" of  column family \"{}\".",
                    property_name,
                    cf_name,
                )
            })
    }

    /// Creates new physical DB checkpoint in directory specified by `path`.
    pub fn create_checkpoint<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        rocksdb::checkpoint::Checkpoint::new(&self.db())?.create_checkpoint(path)?;
        Ok(())
    }

    /// takes a closure with transaction code and retries as required
    pub fn with_optimistic_txn<Fn, Ret, RErr>(
        &self,
        retry: TransactionRetry,
        mut cb: Fn,
    ) -> Result<Ret, TransactionError<RErr>>
    where
        Fn: FnMut(&TransactionCtx) -> Result<Ret, RErr>,
    {
        let mut tries = match retry {
            TransactionRetry::Never => 1,
            TransactionRetry::Count(count) => count,
        };

        while tries > 0 {
            tries -= 1;
            let txn = self.db().transaction();
            let ctx = TransactionCtx {
                txn: &txn,
                db: self,
            };
            let ret = match cb(&ctx) {
                Ok(ret_val) => ret_val,
                Err(err) => return Err(TransactionError::User(err)),
            };

            if let Err(err) = txn.commit() {
                match err.kind() {
                    rocksdb::ErrorKind::Busy | rocksdb::ErrorKind::TryAgain => continue,
                    _ => return Err(TransactionError::ErrorKind(err.kind())),
                }
            }

            return Ok(ret);
        }

        Err(TransactionError::MaxRetriesExceeded)
    }
}

/// Instruction on
pub enum TransactionRetry {
    /// Only try once
    Never,
    /// Specify max retry count
    Count(u16),
}

/// error return for transaction
pub enum TransactionError<RollbackError> {
    /// custom error specified on call
    User(RollbackError),
    /// max retries exceeded
    MaxRetriesExceeded,
    /// other rocksdb related error
    ErrorKind(rocksdb::ErrorKind),
}

impl<T> Into<anyhow::Error> for TransactionError<T>
where
    T: Into<anyhow::Error>,
{
    fn into(self) -> anyhow::Error {
        match self {
            TransactionError::User(err) => err.into(),
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

/// operations inside transaction
pub struct TransactionCtx<'db> {
    txn: &'db rocksdb::Transaction<'db, rocksdb::OptimisticTransactionDB>,
    db: &'db OptimisticTransactionDB,
}

impl<'db> TransactionCtx<'db> {
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
