use std::path::Path;
use std::sync::{Arc, LockResult, RwLock, RwLockReadGuard};

use super::iterator::ScanDirection;
pub use super::iterator::{SchemaIterator, SeekKeyEncoder};
use crate::metrics::{
    SCHEMADB_BATCH_COMMIT_BYTES, SCHEMADB_BATCH_COMMIT_LATENCY_SECONDS, SCHEMADB_DELETES,
    SCHEMADB_GET_BYTES, SCHEMADB_GET_LATENCY_SECONDS, SCHEMADB_PUT_BYTES,
};
use crate::{default_write_options, is_range_bounds_inverse, Operation, SchemaKey};
use anyhow::format_err;
pub use rocksdb;
pub use rocksdb::DEFAULT_COLUMN_FAMILY_NAME;
use rocksdb::{DBAccess, ReadOptions};
use tracing::info;

use super::iterator::RawDbIter;
pub use crate::schema::Schema;
use crate::schema::{ColumnFamilyName, KeyCodec, ValueCodec};
pub use crate::schema_batch::SchemaBatch;

#[allow(missing_docs)]
pub trait RocksDBCommon {
    type WriteBatch: WriteBatch;
    type RDB: DBAccess;

    fn inner(&self) -> &Self::RDB;

    fn write_batch_default(&self) -> Self::WriteBatch;

    fn cf_handle(&self, name: &str) -> Option<&rocksdb::ColumnFamily>;
    fn get_pinned_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl rocksdb::AsColumnFamilyRef,
        key: K,
    ) -> Result<Option<rocksdb::DBPinnableSlice>, rocksdb::Error>;
    fn write_opt(
        &self,
        batch: Self::WriteBatch,
        writeopts: &rocksdb::WriteOptions,
    ) -> Result<(), rocksdb::Error>;

    fn raw_iterator_cf_opt<'a: 'b, 'b>(
        &'a self,
        cf_handle: &impl rocksdb::AsColumnFamilyRef,
        readopts: rocksdb::ReadOptions,
    ) -> rocksdb::DBRawIteratorWithThreadMode<'b, Self::RDB>;
}

struct DB(rocksdb::DB);
impl RocksDBCommon for DB {
    type WriteBatch = rocksdb::WriteBatch;
    type RDB = rocksdb::DB;

    fn inner(&self) -> &Self::RDB {
        &self.0
    }

    fn cf_handle(&self, name: &str) -> Option<&rocksdb::ColumnFamily> {
        rocksdb::DB::cf_handle(&self.0, name)
    }

    fn get_pinned_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl rocksdb::AsColumnFamilyRef,
        key: K,
    ) -> Result<Option<rocksdb::DBPinnableSlice>, rocksdb::Error> {
        rocksdb::DB::get_pinned_cf(&self.0, cf, key)
    }

    fn write_opt(
        &self,
        batch: Self::WriteBatch,
        writeopts: &rocksdb::WriteOptions,
    ) -> Result<(), rocksdb::Error> {
        rocksdb::DB::write_opt(&self.0, batch, writeopts)
    }

    fn write_batch_default(&self) -> Self::WriteBatch {
        Self::WriteBatch::default()
    }

    fn raw_iterator_cf_opt<'a: 'b, 'b>(
        &'a self,
        cf_handle: &impl rocksdb::AsColumnFamilyRef,
        readopts: rocksdb::ReadOptions,
    ) -> rocksdb::DBRawIteratorWithThreadMode<'b, Self::RDB> {
        rocksdb::DB::raw_iterator_cf_opt(&self.0, cf_handle, readopts)
    }
}

struct TransactionDB(rocksdb::TransactionDB);
impl RocksDBCommon for TransactionDB {
    type WriteBatch = rocksdb::WriteBatchWithTransaction<true>;
    type RDB = rocksdb::TransactionDB;

    fn inner(&self) -> &Self::RDB {
        &self.0
    }

    fn cf_handle(&self, name: &str) -> Option<&rocksdb::ColumnFamily> {
        rocksdb::TransactionDB::cf_handle(&self.0, name)
    }

    fn get_pinned_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl rocksdb::AsColumnFamilyRef,
        key: K,
    ) -> Result<Option<rocksdb::DBPinnableSlice>, rocksdb::Error> {
        rocksdb::TransactionDB::get_pinned_cf(&self.0, cf, key)
    }

    fn write_opt(
        &self,
        batch: Self::WriteBatch,
        writeopts: &rocksdb::WriteOptions,
    ) -> Result<(), rocksdb::Error> {
        rocksdb::TransactionDB::write_opt(&self.0, batch, writeopts)
    }

    fn write_batch_default(&self) -> Self::WriteBatch {
        Self::WriteBatch::default()
    }

    fn raw_iterator_cf_opt<'a: 'b, 'b>(
        &'a self,
        cf_handle: &impl rocksdb::AsColumnFamilyRef,
        readopts: rocksdb::ReadOptions,
    ) -> rocksdb::DBRawIteratorWithThreadMode<'b, Self::RDB> {
        rocksdb::TransactionDB::raw_iterator_cf_opt(&self.0, cf_handle, readopts)
    }
}

struct OptimisticTransactionDB(rocksdb::OptimisticTransactionDB);
impl RocksDBCommon for OptimisticTransactionDB {
    type WriteBatch = rocksdb::WriteBatchWithTransaction<true>;
    type RDB = rocksdb::OptimisticTransactionDB;

    fn inner(&self) -> &Self::RDB {
        &self.0
    }

    fn cf_handle(&self, name: &str) -> Option<&rocksdb::ColumnFamily> {
        rocksdb::OptimisticTransactionDB::cf_handle(&self.0, name)
    }

    fn get_pinned_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl rocksdb::AsColumnFamilyRef,
        key: K,
    ) -> Result<Option<rocksdb::DBPinnableSlice>, rocksdb::Error> {
        rocksdb::OptimisticTransactionDB::get_pinned_cf(&self.0, cf, key)
    }

    fn write_opt(
        &self,
        batch: Self::WriteBatch,
        writeopts: &rocksdb::WriteOptions,
    ) -> Result<(), rocksdb::Error> {
        rocksdb::OptimisticTransactionDB::write_opt(&self.0, batch, writeopts)
    }

    fn write_batch_default(&self) -> Self::WriteBatch {
        Self::WriteBatch::default()
    }

    fn raw_iterator_cf_opt<'a: 'b, 'b>(
        &'a self,
        cf_handle: &impl rocksdb::AsColumnFamilyRef,
        readopts: rocksdb::ReadOptions,
    ) -> rocksdb::DBRawIteratorWithThreadMode<'b, Self::RDB> {
        rocksdb::OptimisticTransactionDB::raw_iterator_cf_opt(&self.0, cf_handle, readopts)
    }
}

pub trait WriteBatch {
    fn put_cf<K, V>(&mut self, cf: &impl rocksdb::AsColumnFamilyRef, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>;

    fn delete_cf<K: AsRef<[u8]>>(&mut self, cf: &impl rocksdb::AsColumnFamilyRef, key: K);

    fn size_in_bytes(&self) -> usize;
}

impl<const T: bool> WriteBatch for rocksdb::WriteBatchWithTransaction<T> {
    fn put_cf<K, V>(&mut self, cf: &impl rocksdb::AsColumnFamilyRef, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        rocksdb::WriteBatchWithTransaction::<T>::put_cf(self, cf, key, value)
    }

    fn delete_cf<K: AsRef<[u8]>>(&mut self, cf: &impl rocksdb::AsColumnFamilyRef, key: K) {
        rocksdb::WriteBatchWithTransaction::delete_cf(self, cf, key)
    }

    fn size_in_bytes(&self) -> usize {
        rocksdb::WriteBatchWithTransaction::size_in_bytes(self)
    }
}

#[allow(missing_docs)]
pub struct CommonDB<R: RocksDBCommon> {
    name: &'static str, // for logging
    inner: R,
}

impl<R: 'static + RocksDBCommon> CommonDB<R> {
    fn log_construct(name: &'static str, inner: R) -> Self {
        info!(rocksdb_name = name, "Opened RocksDB");
        Self { name, inner }
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
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;

        let result = self.inner.get_pinned_cf(cf_handle, k)?;
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
        let mut batch = SchemaBatch::new();
        batch.put::<S>(key, value)?;
        self.write_schemas(batch)
    }

    /// Delete a single key from the database.
    pub fn delete<S: Schema>(&self, key: &impl KeyCodec<S>) -> anyhow::Result<()> {
        // Not necessary to use a batch, but we'd like a central place to bump counters.
        // Used in tests only anyway.
        let mut batch = SchemaBatch::new();
        batch.delete::<S>(key)?;
        self.write_schemas(batch)
    }

    /// Writes a group of records wrapped in a [`SchemaBatch`].
    pub fn write_schemas(&self, batch: SchemaBatch) -> anyhow::Result<()> {
        let _timer = SCHEMADB_BATCH_COMMIT_LATENCY_SECONDS
            .with_label_values(&[self.name])
            .start_timer();
        let mut db_batch = self.inner.write_batch_default();
        for (cf_name, rows) in batch.last_writes.iter() {
            let cf_handle = self.get_cf_handle(cf_name)?;
            for (key, operation) in rows {
                match operation {
                    Operation::Put { value } => db_batch.put_cf(cf_handle, key, value),
                    Operation::Delete => db_batch.delete_cf(cf_handle, key),
                }
            }
        }
        let serialized_size = db_batch.size_in_bytes();

        self.inner.write_opt(db_batch, &default_write_options())?;

        // Bump counters only after DB write succeeds.
        for (cf_name, rows) in batch.last_writes.iter() {
            for (key, operation) in rows {
                match operation {
                    Operation::Put { value } => {
                        SCHEMADB_PUT_BYTES
                            .with_label_values(&[cf_name])
                            .observe((key.len() + value.len()) as f64);
                    }
                    Operation::Delete => {
                        SCHEMADB_DELETES.with_label_values(&[cf_name]).inc();
                    }
                }
            }
        }
        SCHEMADB_BATCH_COMMIT_BYTES
            .with_label_values(&[self.name])
            .observe(serialized_size as f64);

        Ok(())
    }

    fn get_cf_handle(&self, cf_name: &str) -> anyhow::Result<&rocksdb::ColumnFamily> {
        self.inner.cf_handle(cf_name).ok_or_else(|| {
            format_err!(
                "DB::cf_handle not found for column family name: {}",
                cf_name
            )
        })
    }
}

impl CommonDB<DB> {
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
        let inner = rocksdb::DB::open_cf_descriptors(db_opts, path, cfds)?;
        Ok(Self::log_construct(name, DB(inner)))
    }

    /// Open db in readonly mode. This db is completely static, so any writes that occur on the primary
    /// after it has been opened will not be visible to the readonly instance.
    pub fn open_cf_readonly(
        opts: &rocksdb::Options,
        path: impl AsRef<Path>,
        name: &'static str,
        cfs: Vec<ColumnFamilyName>,
    ) -> anyhow::Result<Self> {
        let error_if_log_file_exists = false;
        let inner = rocksdb::DB::open_cf_for_read_only(opts, path, cfs, error_if_log_file_exists)?;

        Ok(Self::log_construct(name, DB(inner)))
    }

    /// Open db in secondary mode. A secondary db is does not support writes, but can be dynamically caught up
    /// to the primary instance by a manual call. See <https://github.com/facebook/rocksdb/wiki/Read-only-and-Secondary-instances>
    /// for more details.
    pub fn open_cf_as_secondary<P: AsRef<Path>>(
        opts: &rocksdb::Options,
        primary_path: P,
        secondary_path: P,
        name: &'static str,
        cfs: Vec<ColumnFamilyName>,
    ) -> anyhow::Result<Self> {
        let inner = rocksdb::DB::open_cf_as_secondary(opts, primary_path, secondary_path, cfs)?;
        Ok(Self::log_construct(name, DB(inner)))
    }

    /// Removes the database entries in the range `["from", "to")` using default write options.
    ///
    /// Note that this operation will be done lexicographic on the *encoding* of the seek keys. It is
    /// up to the table creator to ensure that the lexicographic ordering of the encoded seek keys matches the
    /// logical ordering of the type.
    pub fn delete_range<S: Schema>(
        &self,
        from: &impl SeekKeyEncoder<S>,
        to: &impl SeekKeyEncoder<S>,
    ) -> anyhow::Result<()> {
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        let from = from.encode_seek_key()?;
        let to = to.encode_seek_key()?;
        self.inner.inner().delete_range_cf(cf_handle, from, to)?;
        Ok(())
    }

    fn iter_with_direction<S: Schema>(
        &self,
        opts: ReadOptions,
        direction: ScanDirection,
    ) -> anyhow::Result<SchemaIterator<S, DB>> {
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        Ok(SchemaIterator::new(
            self.inner.raw_iterator_cf_opt(cf_handle, opts),
            direction,
        ))
    }

    /// Returns a forward [`SchemaIterator`] on a certain schema with the default read options.
    pub fn iter<S: Schema>(&self) -> anyhow::Result<SchemaIterator<S, DB>> {
        self.iter_with_direction::<S>(Default::default(), ScanDirection::Forward)
    }

    ///  Returns a [`RawDbIter`] which allows to iterate over raw values in specified [`ScanDirection`].
    pub(crate) fn raw_iter<S: Schema>(
        &self,
        direction: ScanDirection,
    ) -> anyhow::Result<RawDbIter<DB>> {
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        Ok(RawDbIter::new(&self.inner, cf_handle, .., direction))
    }

    /// Get a [`RawDbIter`] in given range and direction.
    pub(crate) fn raw_iter_range<S: Schema>(
        &self,
        range: impl std::ops::RangeBounds<SchemaKey>,
        direction: ScanDirection,
    ) -> anyhow::Result<RawDbIter<DB>> {
        if is_range_bounds_inverse(&range) {
            anyhow::bail!("lower_bound > upper_bound");
        }
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        Ok(RawDbIter::new(&self.inner, cf_handle, range, direction))
    }

    /// Returns a forward [`SchemaIterator`] on a certain schema with the provided read options.
    pub fn iter_with_opts<S: Schema>(
        &self,
        opts: ReadOptions,
    ) -> anyhow::Result<SchemaIterator<S, DB>> {
        self.iter_with_direction::<S>(opts, ScanDirection::Forward)
    }

    /// Flushes [MemTable](https://github.com/facebook/rocksdb/wiki/MemTable) data.
    /// This is only used for testing `get_approximate_sizes_cf` in unit tests.
    pub fn flush_cf(&self, cf_name: &str) -> anyhow::Result<()> {
        Ok(self.inner.inner().flush_cf(self.get_cf_handle(cf_name)?)?)
    }

    /// Returns the current RocksDB property value for the provided column family name
    /// and property name.
    pub fn get_property(&self, cf_name: &str, property_name: &str) -> anyhow::Result<u64> {
        self.inner
            .inner()
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
        rocksdb::checkpoint::Checkpoint::new(&self.inner.inner())?.create_checkpoint(path)?;
        Ok(())
    }
}

impl CommonDB<OptimisticTransactionDB> {
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
        let inner = rocksdb::OptimisticTransactionDB::open_cf_descriptors(db_opts, path, cfds)?;
        Ok(Self::log_construct(name, OptimisticTransactionDB(inner)))
    }

    fn iter_with_direction<S: Schema>(
        &self,
        opts: ReadOptions,
        direction: ScanDirection,
    ) -> anyhow::Result<SchemaIterator<S, OptimisticTransactionDB>> {
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        Ok(SchemaIterator::new(
            self.inner.raw_iterator_cf_opt(cf_handle, opts),
            direction,
        ))
    }

    /// Returns a forward [`SchemaIterator`] on a certain schema with the default read options.
    pub fn iter<S: Schema>(&self) -> anyhow::Result<SchemaIterator<S, OptimisticTransactionDB>> {
        self.iter_with_direction::<S>(Default::default(), ScanDirection::Forward)
    }

    ///  Returns a [`RawDbIter`] which allows to iterate over raw values in specified [`ScanDirection`].
    pub(crate) fn raw_iter<S: Schema>(
        &self,
        direction: ScanDirection,
    ) -> anyhow::Result<RawDbIter<OptimisticTransactionDB>> {
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        Ok(RawDbIter::new(&self.inner, cf_handle, .., direction))
    }

    /// Get a [`RawDbIter`] in given range and direction.
    pub(crate) fn raw_iter_range<S: Schema>(
        &self,
        range: impl std::ops::RangeBounds<SchemaKey>,
        direction: ScanDirection,
    ) -> anyhow::Result<RawDbIter<OptimisticTransactionDB>> {
        if is_range_bounds_inverse(&range) {
            anyhow::bail!("lower_bound > upper_bound");
        }
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        Ok(RawDbIter::new(&self.inner, cf_handle, range, direction))
    }

    /// Returns a forward [`SchemaIterator`] on a certain schema with the provided read options.
    pub fn iter_with_opts<S: Schema>(
        &self,
        opts: ReadOptions,
    ) -> anyhow::Result<SchemaIterator<S, OptimisticTransactionDB>> {
        self.iter_with_direction::<S>(opts, ScanDirection::Forward)
    }

    /// Flushes [MemTable](https://github.com/facebook/rocksdb/wiki/MemTable) data.
    /// This is only used for testing `get_approximate_sizes_cf` in unit tests.
    pub fn flush_cf(&self, cf_name: &str) -> anyhow::Result<()> {
        Ok(self.inner.inner().flush_cf(self.get_cf_handle(cf_name)?)?)
    }

    /// Returns the current RocksDB property value for the provided column family name
    /// and property name.
    pub fn get_property(&self, cf_name: &str, property_name: &str) -> anyhow::Result<u64> {
        self.inner
            .inner()
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
        rocksdb::checkpoint::Checkpoint::new(&self.inner.inner())?.create_checkpoint(path)?;
        Ok(())
    }
}

impl CommonDB<TransactionDB> {
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
            &rocksdb::TransactionDBOptions::default(),
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
        txn_db_opts: &rocksdb::TransactionDBOptions,
    ) -> anyhow::Result<Self> {
        let inner = rocksdb::TransactionDB::open_cf_descriptors(db_opts, txn_db_opts, path, cfds)?;
        Ok(Self::log_construct(name, TransactionDB(inner)))
    }

    fn iter_with_direction<S: Schema>(
        &self,
        opts: ReadOptions,
        direction: ScanDirection,
    ) -> anyhow::Result<SchemaIterator<S, TransactionDB>> {
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        Ok(SchemaIterator::new(
            self.inner.raw_iterator_cf_opt(cf_handle, opts),
            direction,
        ))
    }

    /// Returns a forward [`SchemaIterator`] on a certain schema with the default read options.
    pub fn iter<S: Schema>(&self) -> anyhow::Result<SchemaIterator<S, TransactionDB>> {
        self.iter_with_direction::<S>(Default::default(), ScanDirection::Forward)
    }

    ///  Returns a [`RawDbIter`] which allows to iterate over raw values in specified [`ScanDirection`].
    pub(crate) fn raw_iter<S: Schema>(
        &self,
        direction: ScanDirection,
    ) -> anyhow::Result<RawDbIter<TransactionDB>> {
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        Ok(RawDbIter::new(&self.inner, cf_handle, .., direction))
    }

    /// Get a [`RawDbIter`] in given range and direction.
    pub(crate) fn raw_iter_range<S: Schema>(
        &self,
        range: impl std::ops::RangeBounds<SchemaKey>,
        direction: ScanDirection,
    ) -> anyhow::Result<RawDbIter<TransactionDB>> {
        if is_range_bounds_inverse(&range) {
            anyhow::bail!("lower_bound > upper_bound");
        }
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        Ok(RawDbIter::new(&self.inner, cf_handle, range, direction))
    }

    /// Returns a forward [`SchemaIterator`] on a certain schema with the provided read options.
    pub fn iter_with_opts<S: Schema>(
        &self,
        opts: ReadOptions,
    ) -> anyhow::Result<SchemaIterator<S, TransactionDB>> {
        self.iter_with_direction::<S>(opts, ScanDirection::Forward)
    }
}
