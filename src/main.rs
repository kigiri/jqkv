use std::collections::HashMap;
use std::convert::Infallible;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use http_body_util::{BodyExt, Full};
use hyper::StatusCode;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto;
use jaq_core::Ctx;
use jaq_core::load::Arena;
use jaq_json::Val;
use percent_encoding::percent_decode_str;
use serde::Deserialize;
use serde_json::Value;
use tokio::net::TcpListener;
use url::form_urlencoded;

mod cbor;
mod jq;
mod lmdb;

fn res(status: StatusCode, err: impl Into<Bytes>) -> Response<Full<Bytes>> {
    let message = err.into();
    let mut r = Response::new(Full::new(message));
    *r.status_mut() = status;
    r
}

struct JsonArrayWriter<'a, W: Write> {
    out: &'a mut W,
    first: bool,
}

impl<'a, W: Write> JsonArrayWriter<'a, W> {
    fn new(out: &'a mut W) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        out.write_all(b"[")?;
        Ok(Self { out, first: true })
    }

    fn push_val(&mut self, v: Val) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.first {
            self.first = false;
        } else {
            self.out.write_all(b",")?;
        }
        write!(self.out, "{}", v)?;
        Ok(())
    }

    fn finish(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.out.write_all(b"]")?;
        Ok(())
    }
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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: jaq_core::FilterT<V = Val>,
    W: Write,
{
    let txn = db.env.read_txn()?;
    let iter = db.meta.prefix_iter(&txn, &prefix)?;
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
    for item in results {
        if let Ok(v) = item {
            out.push_val(v)?
        }
    }
    Ok(())
}

#[derive(Deserialize, Debug)]
struct ParseBody {
    prefixes: Vec<String>,
    query: String,
    #[serde(default)]
    from: f64,
}
async fn handle_request(
    req: Request<hyper::body::Incoming>,
    db: Arc<lmdb::DB>,
) -> Result<Response<Full<Bytes>>, Box<dyn std::error::Error + Send + Sync>> {
    let path = req.uri().path().to_string();
    let key = path.strip_prefix('/').unwrap_or("");
    match *req.method() {
        Method::GET => {
            let prefix = if key.ends_with('/') {
                key.as_bytes()
            } else {
                &[]
            };
            if !key.is_empty() && prefix.is_empty() {
                let txn = db.env.read_txn()?;
                let val = db.data.get(&txn, key.as_bytes())?;
                match val {
                    None => Ok(res(StatusCode::NOT_FOUND, "Not Found")),
                    Some(val) => {
                        let json = cbor::decode_to_vec(val)?;
                        Ok(Response::new(Full::new(Bytes::from(json))))
                    }
                }
            } else {
                let search = req.uri().query().unwrap_or("").to_string();
                let prefix_bytes = Bytes::copy_from_slice(prefix);
                let handle = tokio::task::spawn_blocking(
                    move || -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
                        let params: HashMap<String, String> =
                            form_urlencoded::parse(search.as_bytes())
                                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                                .collect();

                        let query =
                            normalize_query(params.get("q").map(|s| s.as_str()).unwrap_or("."));

                        let from: f64 = params
                            .get("from")
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0.0);

                        let arena = Arena::default();
                        let filter = jq::compile(&query, &arena)?;
                        let mut output = Vec::with_capacity(4096);
                        let mut arr = JsonArrayWriter::new(&mut output)?;
                        run_search(db.clone(), prefix_bytes, from, &filter, &mut arr)?;
                        arr.finish()?;
                        Ok(Bytes::from(output))
                    },
                );
                Ok(res(StatusCode::OK, handle.await??))
            }
        }
        Method::POST => {
            let c = req.into_body().collect().await?;
            let body = c.to_bytes();
            if key.is_empty() {
                // decode body
                let deserialized = serde_json::from_slice::<ParseBody>(&body);
                let parsed = match deserialized {
                    Ok(p) => p,
                    Err(e) => {
                        return Ok(res(
                            StatusCode::BAD_REQUEST,
                            format!("Unable to parse JSON body: {:?}", e),
                        ));
                    }
                };

                let handle = tokio::task::spawn_blocking(
                    move || -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
                        let mut output = Vec::with_capacity(4096);
                        let mut arr = JsonArrayWriter::new(&mut output)?;
                        let query = normalize_query(&parsed.query);
                        let arena = Arena::default();
                        let filter = jq::compile(&query, &arena)?;
                        for prefix in parsed.prefixes {
                            let db = db.clone();
                            run_search(
                                db,
                                Bytes::copy_from_slice(prefix.as_bytes()),
                                parsed.from,
                                &filter,
                                &mut arr,
                            )?;
                        }
                        arr.finish()?;
                        Ok(Bytes::from(output))
                    },
                );
                return Ok(res(StatusCode::OK, handle.await??));
            }
            let k = percent_decode_str(key).decode_utf8_lossy();
            let bytes = cbor::encode(&body)?;
            let count = lmdb::update_entry(&db, k.as_ref().as_bytes(), &bytes, None)?;
            let status = if count == 0 {
                StatusCode::NOT_MODIFIED
            } else {
                StatusCode::NO_CONTENT
            };
            Ok(res(status, ""))
        }
        Method::DELETE => {
            if key.is_empty() {
                return Ok(res(StatusCode::BAD_REQUEST, "Missing key"));
            }
            let k = percent_decode_str(key).decode_utf8_lossy();
            let _ = lmdb::delete_entry(&db, k.as_ref().as_bytes())?;
            Ok(res(StatusCode::NO_CONTENT, ""))
        }
        _ => Ok(res(StatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed")),
    }
}

async fn handler(
    req: Request<hyper::body::Incoming>,
    db: Arc<lmdb::DB>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let start = Instant::now();
    match handle_request(req, db).await {
        Ok(r) => {
            println!(
                " - [{}] {}ms",
                r.status().as_str(),
                (start.elapsed().as_micros() as f64) / 1000.0
            );
            Ok(r)
        }
        Err(err) => {
            let message = format!("{:?}", err);
            println!(
                " - ERROR {}ms ({})",
                (start.elapsed().as_micros() as f64) / 1000.0,
                message
            );
            Ok(res(StatusCode::INTERNAL_SERVER_ERROR, message))
        }
    }
}

#[derive(Clone)]
pub struct TokioExecutor;

impl<F> hyper::rt::Executor<F> for TokioExecutor
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        tokio::task::spawn(fut);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = Arc::new(lmdb::DB::new().expect("Unable to init the database"));
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let db = db.clone();
        tokio::task::spawn(async move {
            let svc = service_fn(move |req| {
                println!("{}{}", req.method(), req.uri());
                handler(req, db.clone())
            });

            if let Err(err) = auto::Builder::new(TokioExecutor)
                .serve_connection(io, svc)
                .await
            {
                eprintln!("Error serving connection: {}", err);
            }
        });
    }
}
