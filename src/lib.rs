use std::mem::replace;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use gluesql_core::ast::{ColumnDef, IndexOperator, OrderByExpr};
use gluesql_core::data::{Key, Schema, Value};
use gluesql_core::error::Result as GlueResult;
use gluesql_core::store::{
    AlterTable, CustomFunction, CustomFunctionMut, DataRow, Index, IndexMut, Metadata, RowIter,
    Store, StoreMut, Transaction,
};
pub use gluesql_sled_storage::*;
use sled::transaction::ConflictableTransactionResult;
pub use sled::*;
use tokio::sync::{Notify, RwLock};
use tracing::warn;

use crate::lock::release;

mod lock;

/// Lock and Notify
#[derive(Debug)]
struct StorageInner {
    db: RwLock<SledStorage>,
    in_progress: AtomicBool,
    notify: Notify,
}

#[derive(Clone, Debug)]
pub struct SharedSledStorage {
    state: Arc<StorageInner>, // Combined Mutex for state and Notify for signaling
}

impl SharedSledStorage {
    pub fn new(sled_config: Config) -> eyre::Result<Self> {
        let mut database = gluesql_sled_storage::SledStorage::try_from(sled_config)?;

        match replace(&mut database.state, State::Idle) {
            State::Idle => {}
            ref tx @ State::Transaction { txid, .. } => {
                warn!("recovering from unfinished transaction: {:?}", tx);
                match database.tree.transaction(
                    |tx| -> ConflictableTransactionResult<eyre::Result<()>> {
                        Ok(release(&tx, txid))
                    },
                ) {
                    Err(err) => {
                        warn!("error recovering from unfinished transaction: {:?}", err);
                    }
                    Ok(Err(err)) => {
                        warn!("error recovering from unfinished transaction: {:?}", err);
                    }
                    Ok(Ok(_)) => {
                        warn!("recovered from unfinished transaction");
                    }
                }
            }
        }

        let this = SharedSledStorage {
            state: Arc::new(StorageInner {
                db: RwLock::new(database),
                in_progress: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        };
        Ok(this)
    }
    async fn open_transaction(&self) -> GlueResult<()> {
        let state = &self.state;

        while state
            .in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            // Await notification that the transaction has completed.
            state.notify.notified().await;
        }

        Ok(())
    }
    async fn close_transaction(&self) {
        // Set the transaction as not in progress and notify all waiting.
        let state = &self.state;
        state.in_progress.store(false, Ordering::Release);
        state.notify.notify_one();
    }
}

#[async_trait(?Send)]
impl AlterTable for SharedSledStorage {
    async fn rename_schema(&mut self, table_name: &str, new_table_name: &str) -> GlueResult<()> {
        let mut database = self.state.db.write().await;
        database.rename_schema(table_name, new_table_name).await
    }
    async fn rename_column(
        &mut self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> GlueResult<()> {
        let mut database = self.state.db.write().await;
        database
            .rename_column(table_name, old_column_name, new_column_name)
            .await
    }
    async fn add_column(&mut self, table_name: &str, column_def: &ColumnDef) -> GlueResult<()> {
        let mut database = self.state.db.write().await;
        database.add_column(table_name, column_def).await
    }
    async fn drop_column(
        &mut self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> GlueResult<()> {
        let mut database = self.state.db.write().await;
        database
            .drop_column(table_name, column_name, if_exists)
            .await
    }
}
#[async_trait(?Send)]
impl Transaction for SharedSledStorage {
    async fn begin(&mut self, autocommit: bool) -> GlueResult<bool> {
        self.open_transaction().await?;
        self.state.db.write().await.begin(autocommit).await
    }
    async fn rollback(&mut self) -> GlueResult<()> {
        self.state.db.write().await.rollback().await?;
        self.close_transaction().await;
        Ok(())
    }
    async fn commit(&mut self) -> GlueResult<()> {
        self.state.db.write().await.commit().await?;
        self.close_transaction().await;
        Ok(())
    }
}
/// By implementing `Store` trait, you can run `SELECT` query.
#[async_trait(?Send)]
impl Store for SharedSledStorage {
    async fn fetch_schema(&self, table_name: &str) -> GlueResult<Option<Schema>> {
        self.state.db.read().await.fetch_schema(table_name).await
    }
    async fn fetch_all_schemas(&self) -> GlueResult<Vec<Schema>> {
        self.state.db.read().await.fetch_all_schemas().await
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> GlueResult<Option<DataRow>> {
        self.state.db.read().await.fetch_data(table_name, key).await
    }

    async fn scan_data(&self, table_name: &str) -> GlueResult<RowIter> {
        self.state.db.read().await.scan_data(table_name).await
    }
}
#[async_trait(?Send)]
impl StoreMut for SharedSledStorage {
    async fn insert_schema(&mut self, schema: &Schema) -> GlueResult<()> {
        self.state.db.write().await.insert_schema(schema).await
    }

    async fn delete_schema(&mut self, table_name: &str) -> GlueResult<()> {
        self.state.db.write().await.delete_schema(table_name).await
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> GlueResult<()> {
        self.state
            .db
            .write()
            .await
            .append_data(table_name, rows)
            .await
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> GlueResult<()> {
        self.state
            .db
            .write()
            .await
            .insert_data(table_name, rows)
            .await
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> GlueResult<()> {
        self.state
            .db
            .write()
            .await
            .delete_data(table_name, keys)
            .await
    }
}
#[async_trait(?Send)]
impl Index for SharedSledStorage {
    async fn scan_indexed_data(
        &self,
        table_name: &str,
        index_name: &str,
        asc: Option<bool>,
        cmp_value: Option<(&IndexOperator, Value)>,
    ) -> GlueResult<RowIter> {
        self.state
            .db
            .read()
            .await
            .scan_indexed_data(table_name, index_name, asc, cmp_value)
            .await
    }
}
#[async_trait(?Send)]
impl IndexMut for SharedSledStorage {
    async fn create_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        column: &OrderByExpr,
    ) -> GlueResult<()> {
        self.state
            .db
            .write()
            .await
            .create_index(table_name, index_name, column)
            .await
    }
    async fn drop_index(&mut self, table_name: &str, index_name: &str) -> GlueResult<()> {
        self.state
            .db
            .write()
            .await
            .drop_index(table_name, index_name)
            .await
    }
}
impl Metadata for SharedSledStorage {}
impl CustomFunction for SharedSledStorage {}
impl CustomFunctionMut for SharedSledStorage {}
impl Drop for StorageInner {
    fn drop(&mut self) {
        // if there is an active transaction, rollback
        if self.in_progress.load(Ordering::Acquire) {
            if let Err(err) =
                futures::executor::block_on(async { self.db.write().await.rollback().await })
            {
                warn!("error rolling back transaction: {:?}", err);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use gluesql_core::store::Transaction;

    /// Simple unit test to verify that the `Drop` method is called
    #[tokio::test]
    async fn test_drop() {
        use super::{Config, SharedSledStorage};
        {
            let config = Config::new();
            let mut storage = SharedSledStorage::new(config).unwrap();
            storage.begin(false).await.unwrap();
        }
        println!("SharedSledStorage instance dropped successfully!")
    }
    #[tokio::test]
    async fn test_lock_and_recovery() {
        use super::{Config, SharedSledStorage};
        let config = Config::new();
        let mut storage = SharedSledStorage::new(config).unwrap();
        storage.begin(false).await.unwrap();
        storage.state.in_progress.store(true, Ordering::Release);
        drop(storage);
        let config = Config::new();
        let storage = SharedSledStorage::new(config).unwrap();
        drop(storage)
    }
}
