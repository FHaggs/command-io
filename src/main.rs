mod arena;
mod completion;
mod effects;
mod io;
mod runtime;

use completion::CompletionHandle;
use effects::{RuntimeMessage, TurnEffects};
use io::{AcceptOp, IoResult, IoUringDriver, Operation, RawFdLike, RecvOp, SendOp};
use runtime::{IoContext, IoLoop, Isolate, RuntimeError, Server};
use std::net::TcpListener;
use std::os::fd::AsRawFd;

enum Phase {
    Accepting,
    Reading,
    Writing,
}

struct EchoServer {
    listener: RawFdLike,
    connection: Option<RawFdLike>,
    phase: Phase,
    accept: CompletionHandle,
    recv: CompletionHandle,
    send: CompletionHandle,
}

impl EchoServer {
    fn new(listener: RawFdLike) -> Self {
        Self {
            listener,
            connection: None,
            phase: Phase::Accepting,
            accept: CompletionHandle::INVALID,
            recv: CompletionHandle::INVALID,
            send: CompletionHandle::INVALID,
        }
    }

    fn connection_fd(&self) -> RawFdLike {
        self.connection
            .expect("connection phase requires an accepted socket")
    }

    fn submit_accept(&self, effects: &mut TurnEffects) -> Result<(), RuntimeError> {
        effects.submit(
            self.accept,
            Operation::Accept(AcceptOp {
                listener: self.listener,
            }),
        )?;
        Ok(())
    }

    fn submit_recv(&self, effects: &mut TurnEffects) -> Result<(), RuntimeError> {
        effects.submit(
            self.recv,
            Operation::Recv(RecvOp {
                fd: self.connection_fd(),
                buf: vec![0; 4096],
                flags: 0,
            }),
        )?;
        Ok(())
    }

    fn submit_send(&self, buf: Vec<u8>, effects: &mut TurnEffects) -> Result<(), RuntimeError> {
        effects.submit(
            self.send,
            Operation::Send(SendOp {
                fd: self.connection_fd(),
                buf,
                flags: 0,
            }),
        )?;
        Ok(())
    }

    fn close_connection(&mut self) {
        if let Some(fd) = self.connection.take() {
            unsafe {
                libc::close(fd);
            }
        }
    }

    fn accept_next(&mut self, effects: &mut TurnEffects) -> Result<(), RuntimeError> {
        self.close_connection();
        self.phase = Phase::Accepting;
        self.submit_accept(effects)
    }
}

impl Isolate for EchoServer {
    fn handle(
        &mut self,
        msg: RuntimeMessage,
        io: &mut IoContext<'_>,
        effects: &mut TurnEffects,
    ) -> Result<(), RuntimeError> {
        match msg {
            RuntimeMessage::Init => {
                self.accept = io.acquire().expect("completion arena full");
                self.recv = io.acquire().expect("completion arena full");
                self.send = io.acquire().expect("completion arena full");
                self.submit_accept(effects)?;
            }
            RuntimeMessage::IoCompleted(completion) if completion == self.accept => {
                match io.take_result(completion) {
                    Some(IoResult::Accept(Ok(fd))) => {
                        self.connection = Some(fd);
                        self.phase = Phase::Reading;
                        self.submit_recv(effects)?;
                    }
                    Some(IoResult::Accept(Err(error))) => {
                        eprintln!("accept failed: {error:?}");
                        self.submit_accept(effects)?;
                    }
                    Some(IoResult::Cancelled) => {}
                    Some(_) => unreachable!("accept completion had the wrong result kind"),
                    None => unreachable!("accept completion arrived without a result"),
                }
            }
            RuntimeMessage::IoCompleted(completion) if completion == self.recv => {
                match io.take_result(completion) {
                    Some(IoResult::Recv(Ok(bytes))) if bytes.is_empty() => {
                        self.accept_next(effects)?;
                    }
                    Some(IoResult::Recv(Ok(bytes))) => {
                        self.phase = Phase::Writing;
                        println!(
                            "received {} bytes: {:?}",
                            bytes.len(),
                            String::from_utf8_lossy(&bytes)
                        );
                        self.submit_send(bytes, effects)?;
                    }
                    Some(IoResult::Recv(Err(error))) => {
                        eprintln!("recv failed: {error:?}");
                        self.accept_next(effects)?;
                    }
                    Some(IoResult::Cancelled) => {}
                    Some(_) => unreachable!("recv completion had the wrong result kind"),
                    None => unreachable!("recv completion arrived without a result"),
                }
            }
            RuntimeMessage::IoCompleted(completion) if completion == self.send => {
                match io.take_result(completion) {
                    Some(IoResult::Send(Ok(_))) => {
                        self.phase = Phase::Reading;
                        self.submit_recv(effects)?;
                    }
                    Some(IoResult::Send(Err(error))) => {
                        eprintln!("send failed: {error:?}");
                        self.accept_next(effects)?;
                    }
                    Some(IoResult::Cancelled) => {}
                    Some(_) => unreachable!("send completion had the wrong result kind"),
                    None => unreachable!("send completion arrived without a result"),
                }
            }
            RuntimeMessage::IoCompleted(_) => {}
            RuntimeMessage::Shutdown => effects.destroy_self()?,
        }
        Ok(())
    }

    fn destroy(
        &mut self,
        _io: &mut IoContext<'_>,
        effects: &mut TurnEffects,
    ) -> Result<(), RuntimeError> {
        self.close_connection();
        effects.cancel(self.accept)?;
        effects.cancel(self.recv)?;
        effects.cancel(self.send)?;
        Ok(())
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:8080").expect("failed to bind TCP listener");
    let mut server = Server::new(1, 8, 3);
    let mut io_loop = IoLoop::new(IoUringDriver::new(64).expect("io_uring driver failed"), 3);

    server
        .spawn(EchoServer::new(listener.as_raw_fd()))
        .expect("spawn echo server");
    println!("echo server listening on 127.0.0.1:8080");

    loop {
        server
            .step(&mut io_loop)
            .expect("runtime should step echo server");
        io_loop.step().expect("io loop should step");
    }
}
