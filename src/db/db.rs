use std::path::Path;

use anyhow::format_err;
use tracing::info;

use crate::iterator::SeekKeyEncoder;
use crate::schema::ColumnFamilyName;
use crate::schema::Schema;

use super::{CommonDB, RocksDB};

impl RocksDB for rocksdb::DB {
    type WriteBatch = rocksdb::WriteBatch;

    fn cf_handle(&self, name: &str) -> Option<&rocksdb::ColumnFamily> {
        rocksdb::DB::cf_handle(&self, name)
    }

    fn get_pinned_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl rocksdb::AsColumnFamilyRef,
        key: K,
    ) -> Result<Option<rocksdb::DBPinnableSlice>, rocksdb::Error> {
        rocksdb::DB::get_pinned_cf(&self, cf, key)
    }

    fn write_opt(
        &self,
        batch: Self::WriteBatch,
        writeopts: &rocksdb::WriteOptions,
    ) -> Result<(), rocksdb::Error> {
        rocksdb::DB::write_opt(&self, batch, writeopts)
    }

    fn raw_iterator_cf_opt<'a: 'b, 'b>(
        &'a self,
        cf_handle: &impl rocksdb::AsColumnFamilyRef,
        readopts: rocksdb::ReadOptions,
    ) -> rocksdb::DBRawIteratorWithThreadMode<'b, Self> {
        rocksdb::DB::raw_iterator_cf_opt(&self, cf_handle, readopts)
    }
}

/// This DB is a schematized RocksDB wrapper where all data passed in and out are typed according to
/// [`Schema`]s.
#[derive(Debug)]
pub struct DB {
    name: &'static str,
    db: rocksdb::DB,
}

impl CommonDB for DB {
    type DB = rocksdb::DB;

    fn db(&self) -> &Self::DB {
        &self.db
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl DB {
    fn log_construct(name: &'static str, db: rocksdb::DB) -> Self {
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
        let db = rocksdb::DB::open_cf_descriptors(db_opts, path, cfds)?;
        Ok(Self::log_construct(name, db))
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
        let db = rocksdb::DB::open_cf_for_read_only(opts, path, cfs, error_if_log_file_exists)?;

        Ok(Self::log_construct(name, db))
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
        let db = rocksdb::DB::open_cf_as_secondary(opts, primary_path, secondary_path, cfs)?;
        Ok(Self::log_construct(name, db))
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
        self.db().delete_range_cf(cf_handle, from, to)?;
        Ok(())
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
}
