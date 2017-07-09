extern crate tokio_core;
extern crate tokio_proto;
extern crate sider;

use sider::proto::service::SiderService;
use sider::proto::service::SiderProto;
use tokio_proto::TcpServer;

fn main() {
    // Specify the localhost address
    let addr = "0.0.0.0:12345".parse().unwrap();

    // The builder requires a protocol and an address
    let server = TcpServer::new(SiderProto, addr);

    // We provide a way to *instantiate* the service for each new
    // connection; here, we just immediately return a new instance.
    println!("Sider server is running:");
    server.serve(|| Ok(SiderService));
}
