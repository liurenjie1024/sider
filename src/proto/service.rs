use tokio_io::codec::{Encoder, Decoder, Framed};
use tokio_proto::multiplex::RequestId;
use bytes::BytesMut;
use bytes::BigEndian;
use std::str;
use std::io::Error as IOError;
use bytes::buf::IntoBuf;
use bytes::Buf;
use bytes::BufMut;
use tokio_proto::multiplex::ServerProto;
use tokio_io::{AsyncRead, AsyncWrite};
use std::io::Error as SiderError;
use futures::BoxFuture;
use futures::future;
use futures::Future;
use tokio_service::Service;


pub struct SiderCodec;
pub struct SiderProto;
pub struct SiderService;

pub struct SiderRequest {
    data: String
}

pub struct SiderReponse {
    data: String
}

impl Decoder for SiderCodec {
    type Item = (RequestId, SiderRequest);
    type Error = SiderError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 10 {
            return Ok(None)
        }

        let request_id: u64;
        let payload_len: usize;
        let goon: bool;

        // Make compiler happy
        {
            let mut buf = (src as &BytesMut).into_buf();
            request_id = buf.get_u64::<BigEndian>();
            payload_len = buf.get_u16::<BigEndian>() as usize;
            let left_len = buf.remaining();
            goon = left_len >= payload_len;
        }

        if goon {
            src.split_to(6);
            let data = str::from_utf8(&src.as_ref()[0..payload_len]).unwrap().to_string();
            src.split_to(payload_len);
            Ok(Some((request_id, SiderRequest { data: data })))
        } else {
            Ok(None)
        }
    }
}

impl Encoder for SiderCodec {
    type Item = (RequestId, SiderReponse);
    type Error = SiderError;

    fn encode(&mut self, msg: Self::Item, buf: &mut BytesMut) -> Result<(), Self::Error> {
        let (id, msg) = msg;
        buf.put_u64::<BigEndian>(id);
        buf.put_u16::<BigEndian>(msg.data.len() as u16);
        buf.put_slice(msg.data.as_bytes());

        Ok(())
    }
}

impl<T: AsyncRead + AsyncWrite + 'static> ServerProto<T> for SiderProto {
    type Request = SiderRequest;
    type Response = SiderReponse;

    // `Framed<T, LineCodec>` is the return value
    // of `io.framed(LineCodec)`
    type Transport = Framed<T, SiderCodec>;
    type BindTransport = Result<Self::Transport, IOError>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(SiderCodec))
    }
}

impl Service for SiderService {
    type Request = SiderRequest;
    type Response = SiderReponse;
    type Error = SiderError;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn call(&self, req: Self::Request) -> Self::Future {
        future::ok(SiderReponse { data: req.data } ).boxed()
    }
}
