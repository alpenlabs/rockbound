use std::path::Path;

use tracing::info;

use super::{CommonDB, RocksDB};


#[allow(missing_docs)]
pub struct TransactionDBWrap(rocksdb::TransactionDB);
impl RocksDB for TransactionDBWrap {
    type WriteBatch = rocksdb::WriteBatchWithTransaction<true>;
    type DB = rocksdb::TransactionDB;

    fn db(&self) -> &Self::DB {
        &self.0
    }

    fn cf_handle(&self, name: &str) -> Option<&rocksdb::ColumnFamily> {
        <rocksdb::TransactionDB>::cf_handle(&self.0, name)
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

    fn raw_iterator_cf_opt<'a: 'b, 'b>(
        &'a self,
        cf_handle: &impl rocksdb::AsColumnFamilyRef,
        readopts: rocksdb::ReadOptions,
    ) -> rocksdb::DBRawIteratorWithThreadMode<'b, Self::DB> {
        rocksdb::TransactionDB::raw_iterator_cf_opt(&self.0, cf_handle, readopts)
    }
}

#[allow(missing_docs)]
pub struct TransactionDB {
    name: &'static str,
    inner: TransactionDBWrap,
}

impl CommonDB for TransactionDB {
    type Inner = TransactionDBWrap;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn db(&self) -> &<Self::Inner as RocksDB>::DB {
        &self.inner.db()
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl TransactionDB {
    fn log_construct(name: &'static str, inner: TransactionDBWrap) -> Self {
        info!(rocksdb_name = name, "Opened RocksDB");
        Self { name, inner }
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
        Ok(Self::log_construct(name, TransactionDBWrap(db)))
    }
}
