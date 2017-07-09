#![feature(inclusive_range_syntax)]
#![feature(const_fn)]
#![feature(test)]
#![feature(integer_atomics)]
mod collection;
mod thread;
pub mod proto;

extern crate test;
extern crate rand;
extern crate bytes;
extern crate futures;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_service;
