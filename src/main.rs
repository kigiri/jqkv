use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Frame};
use hyper::header::CONTENT_TYPE;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto;
use percent_encoding::percent_decode_str;
use serde::Deserialize;
use serde_json::json;
use tokio::net::TcpListener;
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

fn error_status(err: &service::ApiError) -> StatusCode {
    match err {
        service::ApiError::NotFound => StatusCode::NOT_FOUND,
        service::ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
        service::ApiError::InvalidQuery(_) => StatusCode::BAD_REQUEST,
        service::ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        service::ApiError::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
        service::ApiError::MissingKey => StatusCode::BAD_REQUEST,
    }
}

fn error_code(err: &service::ApiError) -> &'static str {
    match err {
        service::ApiError::NotFound => "not_found",
        service::ApiError::BadRequest(_) => "bad_request",
        service::ApiError::InvalidQuery(_) => "invalid_query",
        service::ApiError::Internal(_) => "internal_error",
        service::ApiError::MethodNotAllowed => "method_not_allowed",
        service::ApiError::MissingKey => "missing_key",
    }
}

fn error_message(err: &service::ApiError) -> &str {
    match err {
        service::ApiError::NotFound => "Not Found",
        service::ApiError::BadRequest(msg) => msg,
        service::ApiError::InvalidQuery(msg) => msg,
        service::ApiError::Internal(msg) => msg,
        service::ApiError::MethodNotAllowed => "Method Not Allowed",
        service::ApiError::MissingKey => "Missing key",
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

fn res_error(err: service::ApiError) -> Response<RespBody> {
    let payload = json!({
        "error": error_message(&err),
        "code": error_code(&err),
    });
    let body = serde_json::to_vec(&payload).unwrap_or_else(|_| {
        b"{\"error\":\"Internal error\",\"code\":\"internal_error\"}".to_vec()
    });
    res_json(error_status(&err), Bytes::from(body))
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

fn handle_service_error(err: service::ApiError) -> Result<Response<RespBody>, service::ApiError> {
    match err {
        service::ApiError::Internal(_) => Err(err),
        _ => Ok(res_error(err)),
    }
}

async fn handle_request(
    req: Request<hyper::body::Incoming>,
    db: Arc<lmdb::DB>,
) -> Result<Response<RespBody>, service::ApiError> {
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
                let query = params.get("q").map(|s| s.as_str()).unwrap_or(".");
                let from: f64 = params
                    .get("from")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);

                let prefix_bytes = Bytes::copy_from_slice(prefix);
                let stream = service::start_query(db.clone(), vec![prefix_bytes], query.to_string(), from);

                match stream.status_rx.await {
                    Ok(Ok(())) => Ok(set_json_header(Response::new(ChannelBody { rx: stream.rx }.boxed()))),
                    Ok(Err(err)) => handle_service_error(err),
                    Err(_) => Err(service::ApiError::Internal("worker error".to_string())),
                }
            }
        }
        Method::POST => {
            let c = req
                .into_body()
                .collect()
                .await
                .map_err(|e| service::ApiError::Internal(format!("body collect: {}", e)))?;
            let body = c.to_bytes();
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

                let stream = service::start_query(db.clone(), prefixes, parsed.query, parsed.from);
                return match stream.status_rx.await {
                    Ok(Ok(())) => Ok(set_json_header(Response::new(ChannelBody { rx: stream.rx }.boxed()))),
                    Ok(Err(err)) => handle_service_error(err),
                    Err(_) => Err(service::ApiError::Internal("worker error".to_string())),
                };
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
    let db_path = std::env::var("STORE_DB_PATH").ok();
    let db = match db_path.as_deref() {
        Some(path) => Arc::new(
            lmdb::DB::new_with_path(Path::new(path)).expect("Unable to init the database"),
        ),
        None => Arc::new(lmdb::DB::new().expect("Unable to init the database")),
    };
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
