use std::{
    io::{Read, Write, empty},
    sync::Arc,
};

use eyre::Result;
use futures::{StreamExt, lock::Mutex, prelude::*};
use opendal::Operator;

/// Erase all data accessible by the operator. ALL DATA WILL BE LOST FOREVER!
pub async fn erase_all(op: &Operator) -> Result<()> {
    op.delete_try_stream(op.lister_with("/").recursive(true).await?)
        .await?;
    Ok(())
}

/// Save all the data accessible by the operator to a tar.lz4 archive.
pub async fn save<W: Write>(op: &Operator, w: W) -> Result<W> {
    let archive = tar::Builder::new(zstd::Encoder::new(w, zstd::DEFAULT_COMPRESSION_LEVEL)?);
    let archive = Arc::new(Mutex::new(archive));
    op.lister_with("/")
        .recursive(true)
        .await?
        .map(|r| Result::<_, eyre::Report>::Ok(r?))
        .try_for_each({
            let archive = archive.clone();
            move |entry| {
                let archive = archive.clone();
                async move {
                    let path = entry.path();
                    if path.ends_with('/') {
                        let mut header = tar::Header::new_ustar();
                        header.set_path(path)?;
                        header.set_entry_type(tar::EntryType::Directory);
                        header.set_mode(0o755);
                        header.set_cksum();
                        let mut archive = archive.lock().await;
                        archive.append(&header, empty())?;
                    } else {
                        let buffer = op.read(path).await?;
                        let mut header = tar::Header::new_ustar();
                        header.set_path(path)?;
                        header.set_entry_type(tar::EntryType::Regular);
                        header.set_size(buffer.len() as u64);
                        header.set_mode(0o644);
                        header.set_cksum();
                        let mut archive = archive.lock().await;
                        archive.append(&header, buffer)?;
                    }
                    Result::Ok(())
                }
            }
        })
        .await?;
    let archive = Arc::into_inner(archive).unwrap().into_inner();
    Ok(archive.into_inner()?.finish()?)
}

/// Load archived data into the operator without cleaning other files.
pub async fn load<R: Read>(op: &Operator, r: R) -> Result<R> {
    Ok(r)
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Once};

    use super::*;

    use opendal::{layers::LoggingLayer, services::MemoryConfig};

    fn data2() -> Vec<u8> {
        let s: String = (0..10000).map(|i| i.to_string()).collect();
        let mut v = s.as_bytes().to_vec();
        for (i, b) in v.iter_mut().enumerate() {
            *b ^= (i % 256) as u8;
        }
        v
    }

    fn data3() -> Vec<u8> {
        let s: String = (0..1000000).rev().map(|i| i.to_string()).collect();
        let mut v = s.as_bytes().to_vec();
        for (i, b) in v.iter_mut().enumerate() {
            *b ^= ((i + 42) % 256) as u8;
        }
        v
    }

    async fn init() -> Operator {
        static INIT: Once = Once::new();
        INIT.call_once(env_logger::init);
        let op = Operator::from_config(MemoryConfig::default())
            .unwrap()
            .layer(LoggingLayer::default())
            .finish();
        op.write("1", "Hello, world!").await.unwrap();
        op.write("2", data2()).await.unwrap();
        op.write("3", data3()).await.unwrap();
        op
    }

    async fn check_data(op: &Operator) {
        assert_eq!(
            &String::from_utf8(op.read("1").await.unwrap().to_vec()).unwrap(),
            "Hello, world!"
        );
        assert_eq!(op.read("2").await.unwrap().to_vec(), data2());
        assert_eq!(op.read("3").await.unwrap().to_vec(), data3());
    }

    #[tokio::test]
    async fn test_erase_all() {
        let op = init().await;
        erase_all(&op).await.unwrap();
        let files = op.list_with("/").recursive(true).await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn test_save_load() {
        let op = init().await;
        let mut v = Vec::<u8>::new();
        save(&op, Cursor::new(&mut v)).await.unwrap();
        assert!(v.len() > 0);
        tokio::fs::write("/home/nkid00/Zhuan/courses/db/aidb/1.tar.zst", &v)
            .await
            .unwrap();
        let v_clone = v.clone();
        erase_all(&op).await.unwrap();
        load(&op, Cursor::new(&mut v)).await.unwrap();
        assert_eq!(v, v_clone);
        check_data(&op).await;
    }
}
