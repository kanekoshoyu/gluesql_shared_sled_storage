# GlueSQL Shared Sled Storage
The stock SledStorage does not do concurrency. I added RwLock and transaction state on top of `SledStorage` to make it as easy to use as the stock `SharedMemoryStorage`.

- RwLock: maintain each valid transaction, by preventing a new transaction while `Sled` is locked
- TransactionState: prevent rollback, by avoiding/awaiting new transaction while another transaction that has begin but yet to commit
- Drop: prevent rollback on next instance, by commiting transaction upon drop as graceful termination

Set `await_active_transaction` as true to await for the active transaction to commit. Set false to just return error when there is active transaction.
