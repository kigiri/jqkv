use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::io::Write;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::body::{Bytes, Frame};
use hyper::header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto;
use percent_encoding::percent_decode_str;
use serde::Deserialize;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use url::form_urlencoded;

mod cbor;
mod jq;
mod lmdb;
mod streaming;
mod service;

type RespBody = BoxBody<Bytes, Infallible>;
type BoxError = Box<dyn std::error::Error + Send + Sync>;
type AppResult<T> = std::result::Result<T, BoxError>;

struct ChannelBody {
    rx: tokio::sync::mpsc::Receiver<Bytes>,
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

#[derive(Deserialize, Debug)]
struct ParseBody {
    prefixes: Vec<String>,
    query: String,
    #[serde(default)]
    from: f64,
}

fn res_json(status: StatusCode, body: impl Into<Bytes>) -> Response<RespBody> {
    let message = body.into();
    let mut r = Response::new(Full::new(message).boxed());
    *r.status_mut() = status;
    r.headers_mut()
        .insert(CONTENT_TYPE, "application/json; charset=utf-8".parse().unwrap());
    r
}

fn res_error(err: service::ApiError) -> Response<RespBody> {
    let (status, code, message) = err.describe();
    let body = error_body(code, message);
    res_json(status, Bytes::from(body))
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

fn error_body(code: &str, message: &str) -> Vec<u8> {
    let payload = json!({
        "error": message,
        "code": code,
    });
    serde_json::to_vec(&payload).unwrap_or_else(|_| {
        b"{\"error\":\"Internal error\",\"code\":\"internal_error\"}".to_vec()
    })
}

fn handle_service_error(err: service::ApiError) -> Result<Response<RespBody>, service::ApiError> {
    match err {
        service::ApiError::Internal(_) => Err(err),
        _ => Ok(res_error(err)),
    }
}

const DEFAULT_MAX_BODY_BYTES: usize = 1_048_576;
static MAX_BODY_BYTES: AtomicUsize = AtomicUsize::new(DEFAULT_MAX_BODY_BYTES);
static API_KEY: OnceLock<Option<String>> = OnceLock::new();

fn payload_too_large(limit: usize) -> service::ApiError {
    service::ApiError::PayloadTooLarge(format!(
        "Payload too large (max {} bytes)",
        limit
    ))
}

async fn drain_body(mut body: hyper::body::Incoming) {
    while let Some(frame) = body.frame().await {
        if frame.is_err() {
            break;
        }
    }
}

async fn run_streamed_query(
    db: Arc<lmdb::DB>,
    prefixes: Vec<Bytes>,
    query: String,
    from: f64,
) -> Result<Response<RespBody>, service::ApiError> {
    let (tx, rx) = mpsc::channel::<Bytes>(32);
    let (status_tx, status_rx) = oneshot::channel::<Result<(), service::ApiError>>();

    tokio::task::spawn_blocking(move || {
        let mut output = streaming::StreamWriter::new(tx);
        let mut status_tx = Some(status_tx);
        let result = service::run_query_streaming(
            db.clone(),
            prefixes,
            query,
            from,
            &mut output,
            || {
                if let Some(tx) = status_tx.take() {
                    let _ = tx.send(Ok(()));
                }
            },
        );
        if let Err(err) = result {
            if let Some(tx) = status_tx.take() {
                let (_status, code, message) = err.describe();
                let body = error_body(code, message);
                let _ = tx.send(Err(err));
                let _ = output.write_all(&body);
                let _ = output.flush();
            }
        } else {
            let _ = output.flush();
        }
    });

    match status_rx.await {
        Ok(Ok(())) => Ok(set_json_header(Response::new(ChannelBody { rx }.boxed()))),
        Ok(Err(err)) => handle_service_error(err),
        Err(_) => Err(service::ApiError::Internal("worker error".to_string())),
    }
}

async fn handle_request(
    req: Request<hyper::body::Incoming>,
    db: Arc<lmdb::DB>,
) -> Result<Response<RespBody>, service::ApiError> {
    if !is_authorized(&req) {
        return Ok(res_error(service::ApiError::Unauthorized));
    }
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
                let json = service::get_value_json(&db, key.as_bytes())?;
                match json {
                    None => Ok(res_error(service::ApiError::NotFound)),
                    Some(json) => Ok(res_json(StatusCode::OK, Bytes::from(json))),
                }
            } else {
                let search = req.uri().query().unwrap_or("");
                let params: HashMap<String, String> = form_urlencoded::parse(search.as_bytes())
                    .map(|(k, v)| (k.into_owned(), v.into_owned()))
                    .collect();
                let query = params.get("q").cloned().unwrap_or_else(|| ".".to_string());
                let from: f64 = params
                    .get("from")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);

                let prefix_bytes = Bytes::copy_from_slice(prefix);
                run_streamed_query(db.clone(), vec![prefix_bytes], query, from).await
            }
        }
        Method::POST => {
            let limit = MAX_BODY_BYTES.load(Ordering::Relaxed);
            if let Some(len) = req.headers().get(CONTENT_LENGTH)
                && let Ok(len_str) = len.to_str()
                && let Ok(len_val) = len_str.parse::<usize>()
                && len_val > limit
            {
                let body = req.into_body();
                // we drain the body to avoid broken pipe errors from clients
                drain_body(body).await;
                return Ok(res_error(payload_too_large(limit)));
            }
            let limited = Limited::new(req.into_body(), limit);
            let collected = match limited.collect().await {
                Ok(c) => c,
                Err(err) => {
                    if err.downcast_ref::<LengthLimitError>().is_some() {
                        return Ok(res_error(payload_too_large(limit)));
                    }
                    return Err(service::ApiError::Internal(format!("body collect: {}", err)));
                }
            };
            let body = collected.to_bytes();
            if key.is_empty() {
                let parsed = match serde_json::from_slice::<ParseBody>(&body) {
                    Ok(p) => p,
                    Err(e) => {
                        return Ok(res_error(service::ApiError::BadRequest(format!(
                            "Unable to parse JSON body: {:?}",
                            e
                        ))));
                    }
                };

                let prefixes = parsed
                    .prefixes
                    .into_iter()
                    .map(|p| Bytes::copy_from_slice(p.as_bytes()))
                    .collect::<Vec<_>>();

                return run_streamed_query(db.clone(), prefixes, parsed.query, parsed.from).await;
            }
            let k = percent_decode_str(key).decode_utf8_lossy();
            let update = match service::update_value(&db, k.as_ref().as_bytes(), &body) {
                Ok(v) => v,
                Err(err) => return handle_service_error(err),
            };
            let status = match update {
                service::UpdateResult::Unchanged => StatusCode::NOT_MODIFIED,
                service::UpdateResult::Updated => StatusCode::NO_CONTENT,
            };
            Ok(res_empty(status))
        }
        Method::DELETE => {
            if key.is_empty() {
                return Ok(res_error(service::ApiError::MissingKey));
            }
            let k = percent_decode_str(key).decode_utf8_lossy();
            if let Err(err) = service::delete_value(&db, k.as_ref().as_bytes()) {
                return handle_service_error(err);
            }
            Ok(res_empty(StatusCode::NO_CONTENT))
        }
        _ => Ok(res_error(service::ApiError::MethodNotAllowed)),
    }
}

fn is_authorized(req: &Request<hyper::body::Incoming>) -> bool {
    let expected = match API_KEY.get() {
        Some(Some(val)) => val.as_str(),
        _ => return true,
    };

    if let Some(value) = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    {
        if value == expected {
            return true;
        }
        if value.strip_prefix("Bearer ") == Some(expected) {
            return true;
        }
    }

    if req
        .headers()
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .map(|value| value == expected)
        .unwrap_or(false)
    {
        return true;
    }

    false
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
            Ok(res_error(service::ApiError::Internal(message)))
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
    if let Some(parsed) = std::env::var("STORE_MAX_BODY_BYTES")
        .ok()
        .and_then(|val| val.parse::<usize>().ok())
    {
        MAX_BODY_BYTES.store(parsed, Ordering::Relaxed);
    }
    let api_key = std::env::var("STORE_API_KEY")
        .ok()
        .filter(|val| !val.is_empty());
    let _ = API_KEY.set(api_key);
    if API_KEY.get().and_then(|val| val.as_ref()).is_none() {
        eprintln!("Warning: STORE_API_KEY is not set, auth is disabled");
    }
    let db_path = std::env::var("STORE_DB_PATH").ok();
    let db = match db_path.as_deref() {
        Some(path) => Arc::new(
            lmdb::DB::new_with_path(Path::new(path)).expect("Unable to init the database"),
        ),
        None => Arc::new(lmdb::DB::new().expect("Unable to init the database")),
    };
    let port = std::env::var("STORE_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3000);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;
    println!("Listening on http://{}", addr);
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
