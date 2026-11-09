#![forbid(unsafe_code)]
use axum::body::{Body, Bytes};
use hyper::{
    Response,
    rt::{Read, ReadBufCursor, Write},
    service::service_fn,
};
use std::{
    collections::VecDeque,
    convert::Infallible,
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};
#[derive(Default)]
struct Wire {
    input: VecDeque<Vec<u8>>,
    output: Vec<u8>,
}
struct Io(Arc<Mutex<Wire>>);
impl Read for Io {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        mut buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        let mut wire = self.0.lock().unwrap();
        if let Some(data) = wire.input.pop_front() {
            assert!(data.len() <= buf.remaining());
            buf.put_slice(&data);
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}
impl Write for Io {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.lock().unwrap().output.extend_from_slice(data);
        Poll::Ready(Ok(data.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
fn http_probe(parts: &[&[u8]], te: bool) -> String {
    let wire = Arc::new(Mutex::new(Wire::default()));
    let service = service_fn(move |_| async move {
        let body = if te {
            let mut headers = hyper::HeaderMap::new();
            headers.insert("x-proof", "yes".parse().unwrap());
            Body::new(Frames(VecDeque::from([
                hyper::body::Frame::data(Bytes::from_static(b"ok")),
                hyper::body::Frame::trailers(headers),
            ])))
        } else {
            Body::from("ok")
        };
        let mut response = Response::new(body);
        if te {
            response
                .headers_mut()
                .insert("trailer", "x-proof".parse().unwrap());
        }
        Ok::<_, Infallible>(response)
    });
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder.auto_date_header(false);
    let mut connection = Box::pin(builder.serve_connection(Io(wire.clone()), service));
    let mut cx = Context::from_waker(Waker::noop());
    for part in parts {
        wire.lock().unwrap().input.push_back(part.to_vec());
        for _ in 0..4 {
            if let Poll::Ready(result) = connection.as_mut().poll(&mut cx) {
                result.unwrap();
                break;
            }
        }
    }
    let output = wire.lock().unwrap().output.clone();
    String::from_utf8(output).unwrap()
}
struct Frames(VecDeque<hyper::body::Frame<Bytes>>);
impl hyper::body::Body for Frames {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.0.pop_front().map(Ok))
    }
}
fn main() {
    let invalid = &[b'"', 128, b' ', b' ', b' ', b'"'];
    let e = serde_json::from_slice::<Box<serde_json::value::RawValue>>(invalid).unwrap_err();
    println!(
        "raw_slice category={:?} line={} column={}",
        e.classify(),
        e.line(),
        e.column()
    );
    let e = serde_json::from_reader::<_, Box<serde_json::value::RawValue>>(std::io::Cursor::new(
        invalid,
    ))
    .unwrap_err();
    println!(
        "raw_reader category={:?} line={} column={}",
        e.classify(),
        e.line(),
        e.column()
    );
    flush_probe();
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(tls_probe());
    for text in [
        "-0",
        "-0.0",
        "-0e2",
        "1.5",
        "18446744073709551615",
        "-9223372036854775808",
    ] {
        let value: serde_json::Value = serde_json::from_str(text).unwrap();
        println!("json {text}: {}", serde_json::to_string(&value).unwrap());
    }
    let e = serde_json::from_slice::<serde_json::Value>(&[b'"', 128, b' ', b' ', b' ', b'"'])
        .unwrap_err();
    println!(
        "utf8 category={:?} line={} column={}",
        e.classify(),
        e.line(),
        e.column()
    );
    let value = serde_json::Number::from_f64(1e100).unwrap();
    println!(
        "f32 overflow={:?}",
        serde_json::from_value::<f32>(serde_json::Value::Number(value))
    );
    for (name, parts, te) in [
        (
            "canonical",
            vec![b"GET / HTTP/1.1\r\nHost: local\r\n\r\n".as_slice()],
            false,
        ),
        (
            "mixed_fragmented",
            vec![
                b"GET / HTTP/1.1\nHost: local".as_slice(),
                b"\n\r\n".as_slice(),
            ],
            false,
        ),
        (
            "te_list",
            vec![b"GET / HTTP/1.1\r\nHost: local\r\nTE: gzip, Trailers\r\n\r\n".as_slice()],
            true,
        ),
        (
            "te_multiple",
            vec![b"GET / HTTP/1.1\r\nHost: local\r\nTE: gzip\r\nTE: trailers\r\n\r\n".as_slice()],
            true,
        ),
    ] {
        println!("http {name}: {:?}", http_probe(&parts, te));
    }
}
#[derive(Debug)]
struct FixedTime;
impl tokio_rustls::rustls::time_provider::TimeProvider for FixedTime {
    fn current_time(&self) -> Option<tokio_rustls::rustls::pki_types::UnixTime> {
        Some(tokio_rustls::rustls::pki_types::UnixTime::since_unix_epoch(
            std::time::Duration::from_secs(1794182400),
        ))
    }
}
async fn tls_probe() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_rustls::{
        TlsAcceptor, TlsConnector,
        rustls::{
            self,
            pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
        },
    };
    let fixtures = std::env::var("TLS_PROBE_FIXTURES").unwrap();
    let certs =
        CertificateDer::pem_slice_iter(&std::fs::read(format!("{fixtures}/chain.pem")).unwrap())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
    let key = PrivateKeyDer::from_pem_slice(&std::fs::read(format!("{fixtures}/end.key")).unwrap())
        .unwrap();
    let mut server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
    server_config.time_provider = Arc::new(FixedTime);
    let mut roots = rustls::RootCertStore::empty();
    for root in
        CertificateDer::pem_slice_iter(&std::fs::read(format!("{fixtures}/root.pem")).unwrap())
    {
        roots.add(root.unwrap()).unwrap();
    }
    let mut client_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_config.time_provider = Arc::new(FixedTime);
    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    let connector = TlsConnector::from(Arc::new(client_config));
    let (a, b) = tokio::io::duplex(65536);
    let (server, client) = tokio::join!(
        acceptor.accept(a),
        connector.connect("foobar.com".try_into().unwrap(), b)
    );
    let mut server = server.unwrap();
    let mut client = client.unwrap();
    server.write_all(b"aaaa").await.unwrap();
    server.flush().await.unwrap();
    server.write_all(b"bbbb").await.unwrap();
    server.flush().await.unwrap();
    let mut buf = [0; 8];
    let n = client.read(&mut buf).await.unwrap();
    let mut total = n;
    while total < 8 {
        let n = client.read(&mut buf[total..]).await.unwrap();
        assert!(n > 0);
        total += n;
    }
    assert_eq!(&buf, b"aaaabbbb");
    println!("tls client first_read={n} total={total}");
    client.write_all(b"aaaa").await.unwrap();
    client.flush().await.unwrap();
    client.write_all(b"bbbb").await.unwrap();
    client.flush().await.unwrap();
    let n = server.read(&mut buf).await.unwrap();
    let mut total = n;
    while total < 8 {
        let n = server.read(&mut buf[total..]).await.unwrap();
        assert!(n > 0);
        total += n;
    }
    assert_eq!(&buf, b"aaaabbbb");
    println!("tls server first_read={n} total={total}");
}
struct DelayedEnd(u8);
impl hyper::body::Body for DelayedEnd {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, Infallible>>> {
        let step = self.0;
        self.0 += 1;
        match step {
            0 => Poll::Ready(Some(Ok(hyper::body::Frame::data(Bytes::from_static(
                b"ok",
            ))))),
            1 => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            _ => Poll::Ready(None),
        }
    }
}
fn flush_probe() {
    let wire = Arc::new(Mutex::new(Wire::default()));
    wire.lock()
        .unwrap()
        .input
        .push_back(b"GET / HTTP/1.1\r\nHost: local\r\n\r\n".to_vec());
    let service =
        service_fn(|_| async { Ok::<_, Infallible>(Response::new(Body::new(DelayedEnd(0)))) });
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder.auto_date_header(false);
    let mut connection = Box::pin(builder.serve_connection(Io(wire.clone()), service));
    let mut cx = Context::from_waker(Waker::noop());
    let result = connection.as_mut().poll(&mut cx);
    assert!(result.is_pending());
    let output = wire.lock().unwrap().output.clone();
    println!(
        "http flush_first_poll: {:?}",
        String::from_utf8(output).unwrap()
    );
}
