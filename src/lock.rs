use eyre::Result;

use {
    gluesql_core::error::Error,
    serde::{Deserialize, Serialize},
    sled::{
        transaction::{ConflictableTransactionError, TransactionalTree},
        Db,
    },
};

#[derive(Debug, Serialize, Deserialize)]
pub struct TxData {
    pub txid: u64,
    pub alive: bool,
    pub created_at: u128,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Lock {
    pub lock_txid: Option<u64>,
    pub lock_created_at: u128,
    pub gc_txid: Option<u64>,
    // TODO: support serializable transaction isolation level
    // - prev_done_at: u128,
}

pub fn get_txdata_key(txid: u64) -> Vec<u8> {
    "tx_data/"
        .to_owned()
        .into_bytes()
        .into_iter()
        .chain(txid.to_be_bytes().iter().copied())
        .collect::<Vec<_>>()
}
#[allow(dead_code)]
pub fn unregister(tree: &Db, txid: u64) -> Result<()> {
    let key = get_txdata_key(txid);
    let mut tx_data: TxData = tree
        .get(&key)?
        .ok_or_else(|| Error::StorageMsg("conflict - lock does not exist".to_owned()))
        .map(|tx_data| bincode::deserialize(&tx_data))??;

    tx_data.alive = false;

    bincode::serialize(&tx_data).map(|tx_data| tree.insert(key, tx_data))??;

    Ok(())
}

pub fn release(tree: &TransactionalTree, txid: u64) -> Result<()> {
    let Lock {
        gc_txid, lock_txid, ..
    } = tree
        .get("lock/")?
        .map(|l| bincode::deserialize(&l))
        .transpose()
        .map_err(ConflictableTransactionError::Abort)?
        .unwrap_or_default();

    if Some(txid) == lock_txid {
        let lock = Lock {
            lock_txid: None,
            lock_created_at: 0,
            gc_txid,
        };

        bincode::serialize(&lock)
            .map_err(ConflictableTransactionError::Abort)
            .map(|lock| tree.insert("lock/", lock))??;
    }

    let key = get_txdata_key(txid);
    let tx_data: Option<TxData> = tree
        .get(&key)?
        .map(|tx_data| bincode::deserialize(&tx_data))
        .transpose()
        .map_err(ConflictableTransactionError::Abort)?;

    let mut tx_data = match tx_data {
        Some(tx_data) => tx_data,
        None => {
            return Ok(());
        }
    };

    tx_data.alive = false;

    bincode::serialize(&tx_data)
        .map_err(ConflictableTransactionError::Abort)
        .map(|tx_data| tree.insert(key, tx_data))??;

    Ok(())
}
