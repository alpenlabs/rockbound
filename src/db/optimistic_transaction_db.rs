use std::path::Path;

use anyhow::format_err;
use tracing::info;

use super::transaction::{TransactionCtx, TransactionError};
use super::{RocksDBOperations, SchemaDBOperations, SchemaDBOperationsExt};

impl RocksDBOperations for rocksdb::OptimisticTransactionDB {
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

/// This DB is a schematized RocksDB wrapper where all data passed in and out are typed according to
/// [`Schema`]s.
#[derive(Debug)]
pub struct OptimisticTransactionDB {
    name: &'static str,
    db: rocksdb::OptimisticTransactionDB,
}

impl SchemaDBOperations for OptimisticTransactionDB {
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
    pub fn with_optimistic_txn<Fn, Ret, RErr: Into<anyhow::Error>>(
        &self,
        retry: TransactionRetry,
        mut cb: Fn,
    ) -> Result<Ret, TransactionError<RErr>>
    where
        Fn: FnMut(&TransactionCtx<Self>) -> Result<Ret, RErr>,
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
                Err(err) => return Err(TransactionError::Rollback(err)),
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        define_schema,
        test::TestField,
        utils::{get_first, get_last},
        Schema,
    };

    define_schema!(TestSchema1, TestField, TestField, "TestCF1");

    fn open_db() -> OptimisticTransactionDB {
        let tmpdir = tempfile::tempdir().unwrap();

        let column_families = vec!["default", TestSchema1::COLUMN_FAMILY_NAME];
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        OptimisticTransactionDB::open(&tmpdir, "test", column_families, &db_opts).unwrap()
    }

    #[test]
    fn test_transaction_commit() {
        let db = open_db();

        let txn_res = db.with_optimistic_txn(TransactionRetry::Never, |ctx| {
            ctx.put::<TestSchema1>(&TestField(1), &TestField(1))?;

            Ok::<_, anyhow::Error>(())
        });

        assert!(txn_res.is_ok());

        let read_value = db.get::<TestSchema1>(&TestField(1));

        assert!(matches!(read_value, Ok(Some(TestField(1)))));
    }

    #[test]
    fn test_transaction_revert() {
        let db = open_db();

        let txn_res = db.with_optimistic_txn(TransactionRetry::Never, |ctx| {
            ctx.put::<TestSchema1>(&TestField(1), &TestField(1))?;

            Err::<(), _>(anyhow::Error::msg("rollback"))
        });

        assert!(matches!(
            txn_res,
            Err(TransactionError::<anyhow::Error>::Rollback(_))
        ));

        let read_value = db.get::<TestSchema1>(&TestField(1));

        assert!(matches!(read_value, Ok(None)));
    }

    #[test]
    fn test_transacton_ctx_iter() {
        let db = open_db();

        let mut batch = crate::SchemaBatch::new();
        batch
            .put::<TestSchema1>(&TestField(3), &TestField(96))
            .unwrap();
        batch
            .put::<TestSchema1>(&TestField(0), &TestField(99))
            .unwrap();
        batch
            .put::<TestSchema1>(&TestField(4), &TestField(95))
            .unwrap();
        batch
            .put::<TestSchema1>(&TestField(1), &TestField(98))
            .unwrap();
        batch
            .put::<TestSchema1>(&TestField(2), &TestField(97))
            .unwrap();

        db.write_schemas(batch).unwrap();

        let first_item = get_first::<TestSchema1, _>(&db);
        let last_item = get_last::<TestSchema1, _>(&db);

        matches!(first_item, Ok(Some((TestField(0), TestField(99)))));
        matches!(last_item, Ok(Some((TestField(4), TestField(95)))));

        let (txn_first_item, txn_last_item) = db
            .with_optimistic_txn(TransactionRetry::Never, |txn| {
                Ok::<_, anyhow::Error>((
                    get_first::<TestSchema1, _>(txn),
                    get_last::<TestSchema1, _>(txn),
                ))
            })
            .unwrap();

        matches!(txn_first_item, Ok(Some((TestField(0), TestField(99)))));
        matches!(txn_last_item, Ok(Some((TestField(4), TestField(95)))));
    }
    // TODO: test retry
}
