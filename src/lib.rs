pub use gluesql_sled_storage::*;
pub use sled::*;

use async_trait::async_trait;
use gluesql_core::ast::{ColumnDef, IndexOperator, OrderByExpr};
use gluesql_core::data::{Key, Schema, Value};
use gluesql_core::error::{Error as GlueError, Result as GlueResult};
use gluesql_core::store::{
    AlterTable, CustomFunction, CustomFunctionMut, DataRow, Index, IndexMut, Metadata, RowIter,
    Store, StoreMut, Transaction,
};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, RwLock};

/// Lock and Notify
type TransactionState = (Mutex<bool>, Notify);
#[derive(Clone, Debug)]
pub struct SharedSledStorage {
    database: Arc<RwLock<SledStorage>>,
    transaction_state: Arc<TransactionState>, // Combined Mutex for state and Notify for signaling
    await_active_transaction: bool, // if set false, collided Transaction::begin() will return error
}

impl SharedSledStorage {
    pub fn new(sled_config: Config, await_active_transaction: bool) -> Self {
        let database = gluesql_sled_storage::SledStorage::try_from(sled_config).unwrap();
        let database = Arc::new(RwLock::new(database));
        SharedSledStorage {
            database,
            transaction_state: Arc::new((Mutex::new(false), Notify::new())),
            await_active_transaction,
        }
    }
    async fn open_transaction(&self) -> GlueResult<()> {
        let (lock, notify) = &*self.transaction_state;
        let mut in_progress = lock.lock().await;
        if !self.await_active_transaction && *in_progress {
            return Err(GlueError::StorageMsg(
                "other transaction in progress".to_string(),
            ));
        }
        while *in_progress {
            // Drop the lock to allow others to modify the flag.
            drop(in_progress);
            // Await notification that the transaction has completed.
            notify.notified().await;
            // Re-acquire the lock to check the flag again.
            in_progress = lock.lock().await;
        }
        // Mark the transaction as started
        *in_progress = true;
        Ok(())
    }
    async fn close_transaction(&self) {
        // Set the transaction as not in progress and notify all waiting.
        let (lock, notify) = &*self.transaction_state;
        let mut in_progress = lock.lock().await;
        *in_progress = false;
        notify.notify_waiters();
    }
}

#[async_trait(?Send)]
impl AlterTable for SharedSledStorage {
    async fn rename_schema(&mut self, _table_name: &str, _new_table_name: &str) -> GlueResult<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;
        database.rename_schema(_table_name, _new_table_name).await
    }
    async fn rename_column(
        &mut self,
        _table_name: &str,
        _old_column_name: &str,
        _new_column_name: &str,
    ) -> GlueResult<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;
        database
            .rename_column(_table_name, _old_column_name, _new_column_name)
            .await
    }
    async fn add_column(&mut self, _table_name: &str, _column_def: &ColumnDef) -> GlueResult<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;
        database.add_column(_table_name, _column_def).await
    }
    async fn drop_column(
        &mut self,
        _table_name: &str,
        _column_name: &str,
        _if_exists: bool,
    ) -> GlueResult<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;
        database
            .drop_column(_table_name, _column_name, _if_exists)
            .await
    }
}
#[async_trait(?Send)]
impl Transaction for SharedSledStorage {
    async fn begin(&mut self, _autocommit: bool) -> GlueResult<bool> {
        self.open_transaction().await?;
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;
        database.begin(_autocommit).await
    }
    async fn rollback(&mut self) -> GlueResult<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;
        self.close_transaction().await;
        database.rollback().await
    }
    async fn commit(&mut self) -> GlueResult<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;
        self.close_transaction().await;
        database.commit().await
    }
}
/// By implementing `Store` trait, you can run `SELECT` query.
#[async_trait(?Send)]
impl Store for SharedSledStorage {
    async fn fetch_schema(&self, _table_name: &str) -> GlueResult<Option<Schema>> {
        let database = Arc::clone(&self.database);
        let database = database.read().await;
        database.fetch_schema(_table_name).await
    }
    async fn fetch_all_schemas(&self) -> GlueResult<Vec<Schema>> {
        let database = Arc::clone(&self.database);
        let database = database.read().await;
        database.fetch_all_schemas().await
    }

    async fn fetch_data(&self, _table_name: &str, _key: &Key) -> GlueResult<Option<DataRow>> {
        let database = Arc::clone(&self.database);
        let database = database.read().await;
        database.fetch_data(_table_name, _key).await
    }

    async fn scan_data(&self, _table_name: &str) -> GlueResult<RowIter> {
        let database = Arc::clone(&self.database);
        let database = database.read().await;
        database.scan_data(_table_name).await
    }
}
#[async_trait(?Send)]
impl StoreMut for SharedSledStorage {
    async fn insert_schema(&mut self, _schema: &Schema) -> GlueResult<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;
        database.insert_schema(_schema).await
    }

    async fn delete_schema(&mut self, _table_name: &str) -> GlueResult<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;
        database.delete_schema(_table_name).await
    }

    async fn append_data(&mut self, _table_name: &str, _rows: Vec<DataRow>) -> GlueResult<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;
        database.append_data(_table_name, _rows).await
    }

    async fn insert_data(
        &mut self,
        _table_name: &str,
        _rows: Vec<(Key, DataRow)>,
    ) -> GlueResult<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;
        database.insert_data(_table_name, _rows).await
    }

    async fn delete_data(&mut self, _table_name: &str, _keys: Vec<Key>) -> GlueResult<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;
        database.delete_data(_table_name, _keys).await
    }
}
#[async_trait(?Send)]
impl Index for SharedSledStorage {
    async fn scan_indexed_data(
        &self,
        _table_name: &str,
        _index_name: &str,
        _asc: Option<bool>,
        _cmp_value: Option<(&IndexOperator, Value)>,
    ) -> GlueResult<RowIter> {
        let database = Arc::clone(&self.database);
        let database = database.read().await;
        database
            .scan_indexed_data(_table_name, _index_name, _asc, _cmp_value)
            .await
    }
}
#[async_trait(?Send)]
impl IndexMut for SharedSledStorage {
    async fn create_index(
        &mut self,
        _table_name: &str,
        _index_name: &str,
        _column: &OrderByExpr,
    ) -> GlueResult<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;
        database
            .create_index(_table_name, _index_name, _column)
            .await
    }
    async fn drop_index(&mut self, _table_name: &str, _index_name: &str) -> GlueResult<()> {
        let database = Arc::clone(&self.database);
        let mut database = database.write().await;
        database.drop_index(_table_name, _index_name).await
    }
}
impl Metadata for SharedSledStorage {}
impl CustomFunction for SharedSledStorage {}
impl CustomFunctionMut for SharedSledStorage {}
impl Drop for SharedSledStorage {
    fn drop(&mut self) {
        let this = self.clone();
        // rollback && commit before drop so a transaction is closed
        let (lock, _) = &*this.transaction_state;
        let in_progress = futures::executor::block_on(lock.lock());
        if *in_progress {
            let _ = futures::executor::block_on(self.rollback());
        }
        let _ = futures::executor::block_on(self.commit());
    }
}

#[cfg(test)]
mod tests {
    /// Simple unit test to verify that the `Drop` method is called
    #[test]
    fn test_drop() {
        use super::{Config, SharedSledStorage};
        {
            let config = Config::new();
            SharedSledStorage::new(config, false);
        }
    }
}
