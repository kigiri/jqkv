use std::collections::HashMap;
use std::convert::Infallible;
use std::io::Write;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::StatusCode;
use hyper::body::{Bytes, Frame};
use hyper::header::CONTENT_TYPE;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto;
use jaq_core::Ctx;
use jaq_core::load::Arena;
use jaq_json::Val;
use percent_encoding::percent_decode_str;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use url::form_urlencoded;

mod cbor;
mod jq;
mod lmdb;

type RespBody = BoxBody<Bytes, Infallible>;
type BoxError = Box<dyn std::error::Error + Send + Sync>;
type AppResult<T> = std::result::Result<T, BoxError>;

#[derive(Debug)]
enum ApiError {
    NotFound,
    BadRequest(String),
    InvalidQuery(String),
    Internal(String),
    MethodNotAllowed,
    MissingKey,
}

impl ApiError {
    fn status(&self) -> StatusCode {
        match self {
            ApiError::NotFound => StatusCode::NOT_FOUND,
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::InvalidQuery(_) => StatusCode::BAD_REQUEST,
            ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            ApiError::MissingKey => StatusCode::BAD_REQUEST,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            ApiError::NotFound => "not_found",
            ApiError::BadRequest(_) => "bad_request",
            ApiError::InvalidQuery(_) => "invalid_query",
            ApiError::Internal(_) => "internal_error",
            ApiError::MethodNotAllowed => "method_not_allowed",
            ApiError::MissingKey => "missing_key",
        }
    }

    fn message(&self) -> &str {
        match self {
            ApiError::NotFound => "Not Found",
            ApiError::BadRequest(msg) => msg,
            ApiError::InvalidQuery(msg) => msg,
            ApiError::Internal(msg) => msg,
            ApiError::MethodNotAllowed => "Method Not Allowed",
            ApiError::MissingKey => "Missing key",
        }
    }
}

fn res_json(status: StatusCode, body: impl Into<Bytes>) -> Response<RespBody> {
    let message = body.into();
    let mut r = Response::new(Full::new(message).boxed());
    *r.status_mut() = status;
    r.headers_mut()
        .insert(CONTENT_TYPE, "application/json; charset=utf-8".parse().unwrap());
    r
}

fn res_error(err: ApiError) -> Response<RespBody> {
    let body = error_body(&err);
    res_json(err.status(), Bytes::from(body))
}

fn error_body(err: &ApiError) -> Vec<u8> {
    let payload = json!({
        "error": err.message(),
        "code": err.code(),
    });
    serde_json::to_vec(&payload).unwrap_or_else(|_| {
        b"{\"error\":\"Internal error\",\"code\":\"internal_error\"}".to_vec()
    })
}

fn res_empty(status: StatusCode) -> Response<RespBody> {
    let mut r = Response::new(Full::new(Bytes::new()).boxed());
    *r.status_mut() = status;
    r
}

fn set_json_header(mut r: Response<RespBody>) -> Response<RespBody> {
    r.headers_mut()
        .insert(CONTENT_TYPE, "application/json; charset=utf-8".parse().unwrap());
    r
}

struct ChannelBody {
    rx: mpsc::Receiver<Bytes>,
}

impl hyper::body::Body for ChannelBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        match Pin::new(&mut this.rx).poll_recv(cx) {
            Poll::Ready(Some(bytes)) => Poll::Ready(Some(Ok(Frame::data(bytes)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

struct StreamWriter {
    tx: mpsc::Sender<Bytes>,
    buf: Vec<u8>,
    limit: usize,
}

impl StreamWriter {
    fn new(tx: mpsc::Sender<Bytes>) -> Self {
        let limit = 64 * 1024;
        Self {
            tx,
            buf: Vec::with_capacity(limit),
            limit,
        }
    }

    fn emit(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let out = std::mem::replace(&mut self.buf, Vec::with_capacity(self.limit));
        self.tx.blocking_send(Bytes::from(out)).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "client disconnected")
        })?;
        Ok(())
    }
}

impl Write for StreamWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(buf);
        if self.buf.len() >= self.limit {
            self.emit()?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.emit()
    }
}

struct JsonArrayWriter<'a, W: Write> {
    out: &'a mut W,
    first: bool,
}

impl<'a, W: Write> JsonArrayWriter<'a, W> {
    fn new(out: &'a mut W) -> AppResult<Self> {
        out.write_all(b"[")?;
        Ok(Self { out, first: true })
    }

    fn push_val(&mut self, v: Val) -> AppResult<()> {
        if self.first {
            self.first = false;
        } else {
            self.out.write_all(b",")?;
        }
        write!(self.out, "{}", v)?;
        Ok(())
    }

    fn finish(self) -> AppResult<()> {
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
) -> AppResult<()>
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
    for v in results.flatten() {
        out.push_val(v)?
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
) -> AppResult<Response<RespBody>> {
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
                    None => Ok(res_error(ApiError::NotFound)),
                    Some(val) => {
                        let json = cbor::decode_to_vec(val)?;
                        Ok(res_json(StatusCode::OK, Bytes::from(json)))
                    }
                }
            } else {
                let search = req.uri().query().unwrap_or("").to_string();
                let prefix_bytes = Bytes::copy_from_slice(prefix);

                let (tx, rx) = mpsc::channel::<Bytes>(32);
                let (status_tx, status_rx) = oneshot::channel::<Result<(), ApiError>>();

                tokio::task::spawn_blocking(move || {
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
                    let filter = match jq::compile(&query, &arena) {
                        Ok(f) => f,
                        Err(err) => {
                            let api_err = ApiError::InvalidQuery(err);
                            let body = error_body(&api_err);
                            let _ = status_tx.send(Err(api_err));
                            let _ = tx.blocking_send(Bytes::from(body));
                            return;
                        }
                    };
                    let _ = status_tx.send(Ok(()));

                    let mut output = StreamWriter::new(tx);
                    let mut arr = match JsonArrayWriter::new(&mut output) {
                        Ok(a) => a,
                        Err(_) => return,
                    };

                    let _ = run_search(db.clone(), prefix_bytes, from, &filter, &mut arr);
                    let _ = arr.finish();
                    let _ = output.flush();
                });

                match status_rx.await {
                    Ok(Ok(())) => Ok(set_json_header(Response::new(ChannelBody { rx }.boxed()))),
                    Ok(Err(api_err)) => {
                        let mut r = Response::new(ChannelBody { rx }.boxed());
                        *r.status_mut() = api_err.status();
                        Ok(set_json_header(r))
                    }
                    Err(_) => Ok(res_error(ApiError::Internal(
                        "worker error".to_string(),
                    ))),
                }
            }
        }
        Method::POST => {
            let c = req.into_body().collect().await?;
            let body = c.to_bytes();
            if key.is_empty() {
                let deserialized = serde_json::from_slice::<ParseBody>(&body);
                let parsed = match deserialized {
                    Ok(p) => p,
                    Err(e) => {
                        return Ok(res_error(ApiError::BadRequest(format!(
                            "Unable to parse JSON body: {:?}",
                            e
                        ))));
                    }
                };

                let (tx, rx) = mpsc::channel::<Bytes>(32);
                let (status_tx, status_rx) = oneshot::channel::<Result<(), ApiError>>();

                tokio::task::spawn_blocking(move || {
                    let query = normalize_query(&parsed.query);
                    let arena = Arena::default();
                    let filter = match jq::compile(&query, &arena) {
                        Ok(f) => f,
                        Err(err) => {
                            let api_err = ApiError::InvalidQuery(err);
                            let body = error_body(&api_err);
                            let _ = status_tx.send(Err(api_err));
                            let _ = tx.blocking_send(Bytes::from(body));
                            return;
                        }
                    };
                    let _ = status_tx.send(Ok(()));

                    let mut output = StreamWriter::new(tx);
                    let mut arr = match JsonArrayWriter::new(&mut output) {
                        Ok(a) => a,
                        Err(_) => return,
                    };

                    for prefix in parsed.prefixes {
                        if run_search(
                            db.clone(),
                            Bytes::copy_from_slice(prefix.as_bytes()),
                            parsed.from,
                            &filter,
                            &mut arr,
                        )
                        .is_err()
                        {
                            break;
                        }
                    }

                    let _ = arr.finish();
                    let _ = output.flush();
                });

                return match status_rx.await {
                    Ok(Ok(())) => Ok(set_json_header(Response::new(ChannelBody { rx }.boxed()))),
                    Ok(Err(api_err)) => {
                        let mut r = Response::new(ChannelBody { rx }.boxed());
                        *r.status_mut() = api_err.status();
                        Ok(set_json_header(r))
                    }
                    Err(_) => Ok(res_error(ApiError::Internal(
                        "worker error".to_string(),
                    ))),
                };
            }
            let k = percent_decode_str(key).decode_utf8_lossy();
            let bytes = cbor::encode(&body)?;
            let count = lmdb::update_entry(&db, k.as_ref().as_bytes(), &bytes, None)?;
            let status = if count == 0 {
                StatusCode::NOT_MODIFIED
            } else {
                StatusCode::NO_CONTENT
            };
            Ok(res_empty(status))
        }
        Method::DELETE => {
            if key.is_empty() {
                return Ok(res_error(ApiError::MissingKey));
            }
            let k = percent_decode_str(key).decode_utf8_lossy();
            let _ = lmdb::delete_entry(&db, k.as_ref().as_bytes())?;
            Ok(res_empty(StatusCode::NO_CONTENT))
        }
        _ => Ok(res_error(ApiError::MethodNotAllowed)),
    }
}

async fn handler(
    req: Request<hyper::body::Incoming>,
    db: Arc<lmdb::DB>,
) -> Result<Response<RespBody>, Infallible> {
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
            Ok(res_error(ApiError::Internal(message)))
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
async fn main() -> AppResult<()> {
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
