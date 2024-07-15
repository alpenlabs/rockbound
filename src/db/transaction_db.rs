use std::path::Path;

use tracing::info;

use super::transaction::{TransactionCtx, TransactionError};
use super::{RocksDBOperations, SchemaDBOperations};

impl RocksDBOperations for rocksdb::TransactionDB {
    type WriteBatch = rocksdb::WriteBatchWithTransaction<true>;

    fn cf_handle(&self, name: &str) -> Option<&rocksdb::ColumnFamily> {
        <rocksdb::TransactionDB>::cf_handle(&self, name)
    }

    fn get_pinned_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl rocksdb::AsColumnFamilyRef,
        key: K,
    ) -> Result<Option<rocksdb::DBPinnableSlice>, rocksdb::Error> {
        rocksdb::TransactionDB::get_pinned_cf(&self, cf, key)
    }

    fn write_opt(
        &self,
        batch: Self::WriteBatch,
        writeopts: &rocksdb::WriteOptions,
    ) -> Result<(), rocksdb::Error> {
        rocksdb::TransactionDB::write_opt(&self, batch, writeopts)
    }

    fn raw_iterator_cf_opt<'a: 'b, 'b>(
        &'a self,
        cf_handle: &impl rocksdb::AsColumnFamilyRef,
        readopts: rocksdb::ReadOptions,
    ) -> rocksdb::DBRawIteratorWithThreadMode<'b, Self> {
        rocksdb::TransactionDB::raw_iterator_cf_opt(&self, cf_handle, readopts)
    }
}

/// This DB is a schematized RocksDB wrapper where all data passed in and out are typed according to
/// [`Schema`]s.
pub struct TransactionDB {
    name: &'static str,
    db: rocksdb::TransactionDB,
}

impl SchemaDBOperations for TransactionDB {
    type DB = rocksdb::TransactionDB;

    fn db(&self) -> &Self::DB {
        &self.db
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl TransactionDB {
    fn log_construct(name: &'static str, db: rocksdb::TransactionDB) -> Self {
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
        let txn_db_opts = rocksdb::TransactionDBOptions::default();
        // println!("{:?}", txn_db_opts.);
        let db = Self::open_with_cfds(
            db_opts,
            path,
            name,
            column_families.into_iter().map(|cf_name| {
                let mut cf_opts = rocksdb::Options::default();
                cf_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
                rocksdb::ColumnFamilyDescriptor::new(cf_name, cf_opts)
            }),
            // &rocksdb::Tran÷sactionDBOptions::default(),
            &txn_db_opts,
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
        let db = rocksdb::TransactionDB::open_cf_descriptors(db_opts, txn_db_opts, path, cfds)?;
        Ok(Self::log_construct(name, db))
    }

    /// run RocksDB operations in a transaction
    pub fn with_txn<Fn, Ret, RErr: Into<anyhow::Error>>(
        &self,
        mut cb: Fn,
    ) -> Result<Ret, TransactionError<RErr>>
    where
        Fn: FnMut(&TransactionCtx<Self>) -> Result<Ret, RErr>,
    {
        let txn = self.db().transaction();
        let ctx = TransactionCtx::<Self> {
            txn: &txn,
            db: self,
        };
        let ret = match cb(&ctx) {
            Ok(ret_val) => ret_val,
            Err(err) => {
                return txn
                    .rollback()
                    .map_err(|err| TransactionError::ErrorKind(err.kind()))
                    .and(Err(TransactionError::Rollback(err)));
            }
        };

        if let Err(err) = txn.commit() {
            match err.kind() {
                _ => return Err(TransactionError::ErrorKind(err.kind())),
            }
        }

        return Ok(ret);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::SchemaDBOperationsExt;

    use crate::{define_schema, test::TestField, Schema};

    define_schema!(TestSchema1, TestField, TestField, "TestCF1");

    fn open_db() -> TransactionDB {
        let tmpdir = tempfile::tempdir().unwrap();

        let column_families = vec!["default", TestSchema1::COLUMN_FAMILY_NAME];
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        TransactionDB::open(&tmpdir, "test", column_families, &db_opts).unwrap()
    }

    #[test]
    fn test_transaction_commit() {
        let db = open_db();

        let txn_res = db.with_txn(|ctx| {
            ctx.put::<TestSchema1>(&TestField(1), &TestField(1))?;

            Ok::<_, anyhow::Error>(())
        });

        assert!(txn_res.is_ok());

        let read_value = db.get::<TestSchema1>(&TestField(1));

        assert!(matches!(read_value, Ok(Some(TestField(1)))));
    }

    #[test]
    fn test_transaction_manual_revert() {
        let db = open_db();

        let txn_res = db.with_txn(|ctx| {
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

    // TODO: test transaction failure revert
}
