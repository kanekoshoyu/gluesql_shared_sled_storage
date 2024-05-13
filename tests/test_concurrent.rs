use eyre::Result;
use gluesql_core::prelude::{Glue, Payload};
use gluesql_shared_sled_storage::SharedSledStorage;
use sled::Config;

async fn get_length(table: &mut Glue<SharedSledStorage>) -> Result<usize> {
    let payloads = table.execute("SELECT * FROM t;").await?;
    match payloads.into_iter().next().unwrap() {
        Payload::Select { labels: _, rows } => Ok(rows.len()),
        _ => unreachable!(),
    }
}
#[tokio::test]
async fn test_concurrent_access_local_thread() -> Result<()> {
    let tmp_dir = std::env::temp_dir().join("temp_db");
    let tmp_db_config = Config::new().path(tmp_dir).cache_capacity(256);
    let db = SharedSledStorage::new(tmp_db_config, true)?;
    let mut table = Glue::new(db.clone());
    let _ = table.execute("CREATE TABLE t (a INT);").await;
    let len = get_length(&mut table).await?;
    println!("Before Length: {}", len);
    table.execute("DELETE FROM t;").await?;
    let len = get_length(&mut table).await?;
    println!("After Length: {}", len);
    assert_eq!(len, 0);

    let localset = tokio::task::LocalSet::new();
    localset
        .run_until(async {
            {
                tokio::task::spawn_local(async move {
                    let mut table = Glue::new(db.clone());
                    for i in 0..100 {
                        println!("Inserting {}", i);
                        table
                            .execute(format!("INSERT INTO t (a) VALUES ({});", i).as_str())
                            .await
                            .unwrap();
                        tokio::task::yield_now().await;
                    }
                });
            }

            loop {
                let payloads = table.execute("SELECT * FROM t;").await?;
                match payloads.into_iter().next().unwrap() {
                    Payload::Select { labels: _, rows } => {
                        println!("Rows: {}", rows.len());
                        if rows.len() == 100 {
                            break;
                        }
                    }
                    _ => unreachable!(),
                }
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_concurrent_access() -> Result<()> {
    let tmp_dir = std::env::temp_dir().join("temp_db_concurrent_access");
    let tmp_db_config = Config::new().path(tmp_dir).cache_capacity(256);
    let db = SharedSledStorage::new(tmp_db_config, true)?;
    let total = 150;

    let mut table = Glue::new(db.clone());
    let _ = table.execute("CREATE TABLE t (a INT);").await;
    let len = get_length(&mut table).await?;
    println!("Before Length: {}", len);
    table.execute("DELETE FROM t;").await?;
    let len = get_length(&mut table).await?;
    println!("After Length: {}", len);
    assert_eq!(len, 0);

    let mut handles = vec![];
    for i in 0..total {
        let db_clone = db.clone();
        // Create a new Tokio runtime for each thread
        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let mut table = Glue::new(db_clone);
            rt.block_on(async {
                let _ = match table
                    .execute(format!("INSERT INTO t (a) VALUES ({});", i).as_str())
                    .await
                {
                    Ok(_) => {
                        println!("Inserted {}", i)
                    }
                    Err(e) => panic!("error inserting: {e}"),
                };
            });
        });
        handles.push(handle);
    }

    for _ in 0..total {
        let db_clone = db.clone();
        let handle = std::thread::spawn(move || {
            // Create a new Tokio runtime for each thread
            let rt = tokio::runtime::Runtime::new().unwrap();
            let mut table = Glue::new(db_clone);
            rt.block_on(async {
                match table.execute("SELECT * FROM t;").await {
                    Ok(res) => match res.into_iter().next().unwrap() {
                        Payload::Select { labels: _, rows: _ } => {
                            // println!("Rows: {}", rows.len());
                        }
                        _ => unreachable!(),
                    },
                    Err(e) => panic!("error slecting: {e}"),
                };
            });
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        match handle.join() {
            Ok(_) => {}
            Err(e) => panic!("handle error {e:?}"),
        }
    }

    Ok(())
}
