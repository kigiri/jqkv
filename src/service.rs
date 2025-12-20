use std::sync::Arc;
use std::io::Write;

use hyper::body::Bytes;
use jaq_core::Ctx;
use jaq_core::load::Arena;
use jaq_json::Val;
use serde_json::Value;

use crate::{cbor, jq, lmdb};
use crate::streaming::JsonArrayWriter;

#[derive(Debug)]
pub enum ApiError {
    NotFound,
    BadRequest(String),
    InvalidQuery(String),
    Internal(String),
    MethodNotAllowed,
    MissingKey,
}

impl ApiError {
    pub fn describe(&self) -> (hyper::StatusCode, &'static str, &str) {
        match self {
            ApiError::NotFound => (hyper::StatusCode::NOT_FOUND, "not_found", "Not Found"),
            ApiError::BadRequest(msg) => (hyper::StatusCode::BAD_REQUEST, "bad_request", msg),
            ApiError::InvalidQuery(msg) => {
                (hyper::StatusCode::BAD_REQUEST, "invalid_query", msg)
            }
            ApiError::Internal(msg) => {
                (hyper::StatusCode::INTERNAL_SERVER_ERROR, "internal_error", msg)
            }
            ApiError::MethodNotAllowed => (
                hyper::StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "Method Not Allowed",
            ),
            ApiError::MissingKey => (hyper::StatusCode::BAD_REQUEST, "missing_key", "Missing key"),
        }
    }
}

pub enum UpdateResult {
    Unchanged,
    Updated,
}

pub fn get_value_json(db: &lmdb::DB, key: &[u8]) -> Result<Option<Vec<u8>>, ApiError> {
    let txn = db
        .read_txn()
        .map_err(|e| ApiError::Internal(format!("read_txn: {}", e)))?;
    let val = db
        .data
        .get(&txn.txn, key)
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

pub fn run_query_streaming<W, F>(
    db: Arc<lmdb::DB>,
    prefixes: Vec<Bytes>,
    query: String,
    from: f64,
    out: &mut W,
    on_ready: F,
) -> Result<(), ApiError>
where
    W: Write,
    F: FnOnce(),
{
    let query = normalize_query(&query);
    let arena = Arena::default();
    let filter = jq::compile(&query, &arena).map_err(ApiError::InvalidQuery)?;
    on_ready();

    let mut arr = JsonArrayWriter::new(out)?;
    for prefix in prefixes {
        run_search(db.clone(), prefix, from, &filter, &mut arr)?;
    }
    arr.finish()?;
    Ok(())
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
        .read_txn()
        .map_err(|e| ApiError::Internal(format!("read_txn: {}", e)))?;
    let iter = db
        .meta
        .prefix_iter(&txn.txn, &prefix)
        .map_err(|e| ApiError::Internal(format!("meta.prefix_iter: {}", e)))?;
    let max = (from * 1_000_000.0) as u64;
    let row_iter = iter.flatten().filter_map(|(k, at)| {
        if at < max {
            return None;
        }

        let bytes_opt = match db.data.get(&txn.txn, &k) {
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
