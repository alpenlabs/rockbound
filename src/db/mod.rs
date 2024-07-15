mod db;
mod optimistic_transaction_db;
mod transaction;
mod transaction_db;

pub use db::DB;
pub use optimistic_transaction_db::{OptimisticTransactionDB, TransactionRetry};
pub use transaction::TransactionError;
pub use transaction_db::TransactionDB;

use anyhow::format_err;

use crate::metrics::{
    SCHEMADB_BATCH_COMMIT_BYTES, SCHEMADB_BATCH_COMMIT_LATENCY_SECONDS, SCHEMADB_DELETES,
    SCHEMADB_GET_BYTES, SCHEMADB_GET_LATENCY_SECONDS, SCHEMADB_PUT_BYTES,
};
use crate::schema::{KeyCodec, ValueCodec};
use crate::{
    default_write_options, is_range_bounds_inverse, Operation, Schema, SchemaBatch, SchemaIterator,
    SchemaKey,
};

use super::iterator::{RawDbIter, ScanDirection};

pub trait RocksDBOperations: rocksdb::DBAccess + Sized {
    type WriteBatch: WriteBatch;

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
    ) -> rocksdb::DBRawIteratorWithThreadMode<'b, Self>;
}

/// Common implemnentation for schematized RocksDB wrapper
pub trait SchemaDBOperations: Sized {
    /// RocksDB: rocksdb::DB | rocksdb::OptimisticTransactionDB | rocksdb::TransactionDB
    type DB: RocksDBOperations;

    /// Get reference to rocksdb
    fn db(&self) -> &Self::DB;

    /// Db name
    fn name(&self) -> &str;

    /// (internal) Get ColumnFamily handle for `cf_name``
    fn get_cf_handle(&self, cf_name: &str) -> anyhow::Result<&rocksdb::ColumnFamily> {
        self.db().cf_handle(cf_name).ok_or_else(|| {
            format_err!(
                "DB::cf_handle not found for column family name: {}",
                cf_name
            )
        })
    }

    /// Reads single record by key.
    fn get<S: Schema>(&self, schema_key: &impl KeyCodec<S>) -> anyhow::Result<Option<S::Value>> {
        let _timer = SCHEMADB_GET_LATENCY_SECONDS
            .with_label_values(&[S::COLUMN_FAMILY_NAME])
            .start_timer();

        let k = schema_key.encode_key()?;
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;

        let result = self.db().get_pinned_cf(cf_handle, k)?;
        SCHEMADB_GET_BYTES
            .with_label_values(&[S::COLUMN_FAMILY_NAME])
            .observe(result.as_ref().map_or(0.0, |v| v.len() as f64));

        result
            .map(|raw_value| <S::Value as ValueCodec<S>>::decode_value(&raw_value))
            .transpose()
            .map_err(|err| err.into())
    }

    /// Writes single record.
    fn put<S: Schema>(
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
    fn delete<S: Schema>(&self, key: &impl KeyCodec<S>) -> anyhow::Result<()> {
        // Not necessary to use a batch, but we'd like a central place to bump counters.
        // Used in tests only anyway.
        let mut batch = SchemaBatch::new();
        batch.delete::<S>(key)?;
        self.write_schemas(batch)
    }

    /// Writes a group of records wrapped in a [`SchemaBatch`].
    fn write_schemas(&self, batch: SchemaBatch) -> anyhow::Result<()> {
        let _timer = SCHEMADB_BATCH_COMMIT_LATENCY_SECONDS
            .with_label_values(&[self.name()])
            .start_timer();
        let mut db_batch = <<Self as SchemaDBOperations>::DB as RocksDBOperations>::WriteBatch::default();
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

        self.db().write_opt(db_batch, &default_write_options())?;

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
            .with_label_values(&[self.name()])
            .observe(serialized_size as f64);

        Ok(())
    }

    /// Returns a forward [`SchemaIterator`] on a certain schema with the default read options.
    fn iter<S: Schema>(&self) -> anyhow::Result<SchemaIterator<S, Self>> {
        iter_with_direction::<S, Self>(&self, Default::default(), ScanDirection::Forward)
    }

    /// TODO: pub(crate)
    ///  Returns a [`RawDbIter`] which allows to iterate over raw values in specified [`ScanDirection`].
    fn raw_iter<S: Schema>(&self, direction: ScanDirection) -> anyhow::Result<RawDbIter<Self::DB>> {
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        Ok(RawDbIter::new(&self.db(), cf_handle, .., direction))
    }

    /// TODO: pub(crate)
    /// Get a [`RawDbIter`] in given range and direction.
    fn raw_iter_range<S: Schema>(
        &self,
        range: impl std::ops::RangeBounds<SchemaKey>,
        direction: ScanDirection,
    ) -> anyhow::Result<RawDbIter<Self::DB>> {
        if is_range_bounds_inverse(&range) {
            anyhow::bail!("lower_bound > upper_bound");
        }
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        Ok(RawDbIter::new(&self.db(), cf_handle, range, direction))
    }

    /// Returns a forward [`SchemaIterator`] on a certain schema with the provided read options.
    fn iter_with_opts<S: Schema>(
        &self,
        opts: rocksdb::ReadOptions,
    ) -> anyhow::Result<SchemaIterator<S, Self>> {
        iter_with_direction::<S, Self>(&self, opts, ScanDirection::Forward)
    }
}

pub trait WriteBatch: Default {
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

fn iter_with_direction<S: Schema, D: SchemaDBOperations>(
    this: &D,
    opts: rocksdb::ReadOptions,
    direction: ScanDirection,
) -> anyhow::Result<SchemaIterator<S, D>> {
    let cf_handle = this.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
    Ok(SchemaIterator::new(
        this.db().raw_iterator_cf_opt(cf_handle, opts),
        direction,
    ))
}
