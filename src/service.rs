use std::sync::Arc;
use std::io::Write;

use hyper::body::Bytes;
use jaq_core::Ctx;
use jaq_core::load::Arena;
use jaq_json::Val;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use crate::{cbor, jq, lmdb};
use crate::streaming::{JsonArrayWriter, StreamWriter};

#[derive(Debug)]
pub enum ApiError {
    NotFound,
    BadRequest(String),
    InvalidQuery(String),
    Internal(String),
    MethodNotAllowed,
    MissingKey,
}

pub enum UpdateResult {
    Unchanged,
    Updated,
}

pub struct StreamHandle {
    pub status_rx: oneshot::Receiver<Result<(), ApiError>>,
    pub rx: mpsc::Receiver<Bytes>,
}

pub fn get_value_json(db: &lmdb::DB, key: &[u8]) -> Result<Option<Vec<u8>>, ApiError> {
    let txn = db
        .env
        .read_txn()
        .map_err(|e| ApiError::Internal(format!("read_txn: {}", e)))?;
    let val = db
        .data
        .get(&txn, key)
        .map_err(|e| ApiError::Internal(format!("data.get: {}", e)))?;
    match val {
        None => Ok(None),
        Some(val) => {
            let json =
                cbor::decode_to_vec(val).map_err(|e| ApiError::Internal(e.to_string()))?;
            Ok(Some(json))
        }
    }
}

pub fn update_value(db: &lmdb::DB, key: &[u8], body: &[u8]) -> Result<UpdateResult, ApiError> {
    let bytes = cbor::encode(body)
        .map_err(|e| ApiError::BadRequest(format!("Invalid JSON body: {}", e)))?;
    let count = lmdb::update_entry(db, key, &bytes, None)
        .map_err(|e| ApiError::Internal(format!("update_entry: {}", e)))?;
    if count == 0 {
        Ok(UpdateResult::Unchanged)
    } else {
        Ok(UpdateResult::Updated)
    }
}

pub fn delete_value(db: &lmdb::DB, key: &[u8]) -> Result<(), ApiError> {
    lmdb::delete_entry(db, key)
        .map_err(|e| ApiError::Internal(format!("delete_entry: {}", e)))?;
    Ok(())
}

pub fn start_query(
    db: Arc<lmdb::DB>,
    prefixes: Vec<Bytes>,
    query: String,
    from: f64,
) -> StreamHandle {
    let (tx, rx) = mpsc::channel::<Bytes>(32);
    let (status_tx, status_rx) = oneshot::channel::<Result<(), ApiError>>();

    tokio::task::spawn_blocking(move || {
        let query = normalize_query(&query);
        let arena = Arena::default();
        let filter = match jq::compile(&query, &arena) {
            Ok(f) => f,
            Err(err) => {
                let _ = status_tx.send(Err(ApiError::InvalidQuery(err)));
                return;
            }
        };
        let _ = status_tx.send(Ok(()));

        let mut output = StreamWriter::new(tx);
        let mut arr = match JsonArrayWriter::new(&mut output) {
            Ok(a) => a,
            Err(_) => return,
        };

        for prefix in prefixes {
            if run_search(db.clone(), prefix, from, &filter, &mut arr).is_err() {
                break;
            }
        }

        let _ = arr.finish();
        let _ = output.flush();
    });

    StreamHandle { status_rx, rx }
}


fn normalize_query(query: &str) -> String {
    if query.contains("inputs") {
        query.to_string()
    } else {
        format!("inputs | {}", query)
    }
}

fn run_search<W, F>(
    db: Arc<lmdb::DB>,
    prefix: Bytes,
    from: f64,
    filter: &jaq_core::Filter<F>,
    out: &mut JsonArrayWriter<'_, W>,
) -> Result<(), ApiError>
where
    F: jaq_core::FilterT<V = Val>,
    W: Write,
{
    let txn = db
        .env
        .read_txn()
        .map_err(|e| ApiError::Internal(format!("read_txn: {}", e)))?;
    let iter = db
        .meta
        .prefix_iter(&txn, &prefix)
        .map_err(|e| ApiError::Internal(format!("meta.prefix_iter: {}", e)))?;
    let max = (from * 1_000_000.0) as u64;
    let row_iter = iter.flatten().filter_map(|(k, at)| {
        if at < max {
            return None;
        }

        let bytes_opt = match db.data.get(&txn, &k) {
            Ok(b) => b,
            Err(e) => return Some(Err(format!("data.get error: {}", e))),
        };
        let bytes = match bytes_opt {
            Some(b) => b,
            None => return None,
        };

        let json = match cbor::decode(bytes) {
            Ok(v) => v,
            Err(_) => return None,
        };

        let key_str: String = String::from_utf8_lossy(&k).into_owned();
        let json_with_meta = match json {
            Value::Object(mut map) => {
                map.insert("_key".to_string(), Value::String(key_str));
                map.insert(
                    "_at".to_string(),
                    Value::Number(
                        serde_json::Number::from_f64((at as f64) / 1_000_000.0)
                            .unwrap_or(serde_json::Number::from(0)),
                    ),
                );
                Value::Object(map)
            }
            _ => return None,
        };

        Some(Ok(Val::from(json_with_meta)))
    });

    let inputs = jaq_core::RcIter::new(row_iter);
    let results = filter.run((Ctx::new([], &inputs), Val::Null));
    for v in results.flatten() {
        out.push_val(v)?
    }
    Ok(())
}
