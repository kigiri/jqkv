#[cfg(target_os = "windows")]
#[link(name = "advapi32")]
unsafe extern "C" {}

use ::std::time::*;
use heed::{
    BoxedError, BytesDecode, BytesEncode, Database, Env, EnvOpenOptions, Error as HeedError,
    MdbError,
    byteorder::BigEndian,
    types::{Bytes, U64},
};
use std::borrow::Cow;
use std::fs::create_dir_all;
use std::path::Path;

type DataDb = Database<RawKeyCodec, Bytes>;
type MetaDb = Database<RawKeyCodec, U64<BigEndian>>; // (Microseconds)

pub struct RawPrefixCodec;
pub struct RawKeyCodec;
pub struct DB {
    pub env: Env,
    pub data: DataDb,
    pub meta: MetaDb,
}

impl DB {
    pub fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let path = Path::new("cache");
        Self::new_with_path(path)
    }

    pub fn new_with_path(
        path: &Path,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        create_dir_all(path)?;
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(2560 * 1024 * 1024)
                .max_dbs(2)
                .open(path)?
        };

        let mut wtxn = env.write_txn()?;
        let data = env.create_database(&mut wtxn, Some("data"))?;
        let meta = env.create_database(&mut wtxn, Some("meta"))?;
        data.remap_key_type::<RawPrefixCodec>();
        meta.remap_key_type::<RawPrefixCodec>();
        wtxn.commit()?;
        Ok(Self { env, data, meta })
    }
}

pub fn update_entry(
    db: &DB,
    key: &[u8],
    value: &[u8],
    timestamp_secs: Option<f64>,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let now: u64 = match timestamp_secs {
        Some(ts) => (ts * 1_000_000.0) as u64,
        None => SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as u64,
    };
    for _ in 0..3 {
        let mut wtxn = db.env.write_txn()?;
        if let Some(existing_bytes) = db.data.get(&wtxn, key)? {
            // No changes, nothing to do (compare compressed bytes)
            if existing_bytes == value {
                wtxn.abort();
                return Ok(0);
            }
        }
        if let Err(err) = db.data.put(&mut wtxn, key, value) {
            wtxn.abort();
            if try_resize_map(&db.env, &err) {
                continue;
            }
            return Err(err.into());
        }
        if let Err(err) = db.meta.put(&mut wtxn, key, &now) {
            wtxn.abort();
            if try_resize_map(&db.env, &err) {
                continue;
            }
            return Err(err.into());
        }
        match wtxn.commit() {
            Ok(()) => return Ok(now),
            Err(err) => {
                if try_resize_map(&db.env, &err) {
                    continue;
                }
                return Err(err.into());
            }
        }
    }
    Err("lmdb map resize failed after retries".into())
}

pub fn delete_entry(
    db: &DB,
    key: &[u8],
) -> Result<u64, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let mut wtxn = db.env.write_txn()?;
    db.data.delete(&mut wtxn, key)?;
    db.meta.delete(&mut wtxn, key)?;
    wtxn.commit()?;
    Ok(1)
}

fn try_resize_map(env: &Env, err: &HeedError) -> bool {
    if !matches!(err, HeedError::Mdb(MdbError::MapFull)) {
        return false;
    }
    let current = env.info().map_size;
    let new_size = current.saturating_mul(2);
    if new_size <= current {
        return false;
    }
    unsafe { env.resize(new_size) }.is_ok()
}

impl BytesEncode<'_> for RawKeyCodec {
    type EItem = [u8];

    fn bytes_encode(key: &Self::EItem) -> Result<Cow<'_, [u8]>, BoxedError> {
        Ok(Cow::Borrowed(key))
    }
}

impl BytesDecode<'_> for RawKeyCodec {
    type DItem = Vec<u8>;

    fn bytes_decode(bytes: &'_ [u8]) -> Result<Self::DItem, BoxedError> {
        Ok(bytes.to_vec())
    }
}

impl BytesEncode<'_> for RawPrefixCodec {
    type EItem = [u8];

    fn bytes_encode(prefix: &Self::EItem) -> Result<Cow<'_, [u8]>, BoxedError> {
        Ok(Cow::Borrowed(prefix))
    }
}

impl BytesDecode<'_> for RawPrefixCodec {
    type DItem = Vec<u8>;

    fn bytes_decode(bytes: &'_ [u8]) -> Result<Self::DItem, BoxedError> {
        Ok(bytes.to_vec())
    }
}
