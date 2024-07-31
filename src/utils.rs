//! db utility functions
use crate::{Schema, SchemaDBOperationsExt};

/// Get last last entry in schema, sorted lexically by keys
pub fn get_last<S, DB>(db: &DB) -> anyhow::Result<Option<(S::Key, S::Value)>>
where
    S: Schema,
    DB: SchemaDBOperationsExt,
{
    let mut iterator = db.iter::<S>()?;
    iterator.seek_to_last();
    match iterator.rev().next() {
        Some(res) => {
            let key_val = res?.into_tuple();
            Ok(Some(key_val))
        }
        None => Ok(None),
    }
}

/// Get first stored entry in schema, sorted lexically by keys
pub fn get_first<S, DB>(db: &DB) -> anyhow::Result<Option<(S::Key, S::Value)>>
where
    S: Schema,
    DB: SchemaDBOperationsExt,
{
    let mut iterator = db.iter::<S>()?;
    match iterator.next() {
        Some(res) => {
            let key_val = res?.into_tuple();
            Ok(Some(key_val))
        }
        None => Ok(None),
    }
}
