mod arena;
mod completion;
mod effects;
mod io;
mod runtime;

use completion::CompletionHandle;
use effects::{RuntimeMessage, TurnEffects};
use io::{AcceptOp, IoResult, IoUringDriver, Operation, RawFdLike, RecvOp, SendOp};
use runtime::{IoContext, IoLoop, Isolate, RuntimeError, Server, StepResult};
use std::env;
use std::net::TcpListener;
use std::os::fd::AsRawFd;

const MAX_CONNECTIONS: usize = 64;

struct EchoListener {
    listener: RawFdLike,
    accept: CompletionHandle,
}

impl EchoListener {
    fn new(listener: RawFdLike) -> Self {
        Self {
            listener,
            accept: CompletionHandle::INVALID,
        }
    }

    fn submit_accept(&self, effects: &mut TurnEffects<EchoIsolate>) -> Result<(), RuntimeError> {
        effects.submit(
            self.accept,
            Operation::Accept(AcceptOp {
                listener: self.listener,
            }),
        )?;
        Ok(())
    }
}

struct EchoConnection {
    fd: Option<RawFdLike>,
    recv: CompletionHandle,
    send: CompletionHandle,
}

impl EchoConnection {
    fn new(fd: RawFdLike) -> Self {
        Self {
            fd: Some(fd),
            recv: CompletionHandle::INVALID,
            send: CompletionHandle::INVALID,
        }
    }

    fn fd(&self) -> RawFdLike {
        self.fd.expect("connection must own a socket while active")
    }

    fn submit_recv(&self, effects: &mut TurnEffects<EchoIsolate>) -> Result<(), RuntimeError> {
        effects.submit(
            self.recv,
            Operation::Recv(RecvOp {
                fd: self.fd(),
                buf: vec![0; 4096],
                flags: 0,
            }),
        )?;
        Ok(())
    }

    fn submit_send(
        &self,
        buf: Vec<u8>,
        effects: &mut TurnEffects<EchoIsolate>,
    ) -> Result<(), RuntimeError> {
        effects.submit(
            self.send,
            Operation::Send(SendOp {
                fd: self.fd(),
                buf,
                flags: 0,
            }),
        )?;
        Ok(())
    }

    fn close(&mut self) {
        if let Some(fd) = self.fd.take() {
            unsafe {
                libc::close(fd);
            }
        }
    }
}

impl Drop for EchoConnection {
    fn drop(&mut self) {
        self.close();
    }
}

enum EchoIsolate {
    Listener(EchoListener),
    Connection(EchoConnection),
}

impl Isolate for EchoIsolate {
    fn handle(
        &mut self,
        msg: RuntimeMessage,
        io: &mut IoContext<'_>,
        effects: &mut TurnEffects<Self>,
    ) -> Result<(), RuntimeError> {
        match self {
            Self::Listener(listener) => match msg {
                RuntimeMessage::Init => {
                    listener.accept = io.acquire().expect("completion arena full");
                    listener.submit_accept(effects)?;
                }
                RuntimeMessage::IoCompleted(completion) if completion == listener.accept => {
                    match io.take_result(completion) {
                        Some(IoResult::Accept(Ok(fd))) => {
                            effects.spawn(Self::Connection(EchoConnection::new(fd)))?;
                            listener.submit_accept(effects)?;
                        }
                        Some(IoResult::Accept(Err(error))) => {
                            eprintln!("accept failed: {error:?}");
                            listener.submit_accept(effects)?;
                        }
                        Some(IoResult::Cancelled) => {}
                        Some(_) => unreachable!("accept completion had the wrong result kind"),
                        None => unreachable!("accept completion arrived without a result"),
                    }
                }
                RuntimeMessage::IoCompleted(_) => {}
                RuntimeMessage::Shutdown => effects.destroy_self()?,
            },
            Self::Connection(connection) => match msg {
                RuntimeMessage::Init => {
                    connection.recv = io.acquire().expect("completion arena full");
                    connection.send = io.acquire().expect("completion arena full");
                    connection.submit_recv(effects)?;
                }
                RuntimeMessage::IoCompleted(completion) if completion == connection.recv => {
                    match io.take_result(completion) {
                        Some(IoResult::Recv(Ok(bytes))) if bytes.is_empty() => {
                            effects.destroy_self()?
                        }
                        Some(IoResult::Recv(Ok(bytes))) => {
                            connection.submit_send(bytes, effects)?;
                        }
                        Some(IoResult::Recv(Err(error))) => {
                            eprintln!("recv failed: {error:?}");
                            effects.destroy_self()?;
                        }
                        Some(IoResult::Cancelled) => {}
                        Some(_) => unreachable!("recv completion had the wrong result kind"),
                        None => unreachable!("recv completion arrived without a result"),
                    }
                }
                RuntimeMessage::IoCompleted(completion) if completion == connection.send => {
                    match io.take_result(completion) {
                        Some(IoResult::Send(Ok(_))) => connection.submit_recv(effects)?,
                        Some(IoResult::Send(Err(error))) => {
                            eprintln!("send failed: {error:?}");
                            effects.destroy_self()?;
                        }
                        Some(IoResult::Cancelled) => {}
                        Some(_) => unreachable!("send completion had the wrong result kind"),
                        None => unreachable!("send completion arrived without a result"),
                    }
                }
                RuntimeMessage::IoCompleted(_) => {}
                RuntimeMessage::Shutdown => effects.destroy_self()?,
            },
        }
        Ok(())
    }

    fn destroy(
        &mut self,
        _io: &mut IoContext<'_>,
        effects: &mut TurnEffects<Self>,
    ) -> Result<(), RuntimeError> {
        match self {
            Self::Listener(listener) => effects.cancel(listener.accept)?,
            Self::Connection(connection) => {
                connection.close();
                effects.cancel(connection.recv)?;
                effects.cancel(connection.send)?;
            }
        }
        Ok(())
    }
}

fn main() {
    let address = env::var("COMMAND_IO_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let listener = TcpListener::bind(&address).expect("failed to bind TCP listener");
    let mut server = Server::new(1 + MAX_CONNECTIONS, 1 + MAX_CONNECTIONS, 2);
    let mut io_loop = IoLoop::new(
        IoUringDriver::new((1 + MAX_CONNECTIONS * 2) as u32).expect("io_uring driver failed"),
        1 + MAX_CONNECTIONS * 2,
    );

    server
        .spawn(EchoIsolate::Listener(EchoListener::new(
            listener.as_raw_fd(),
        )))
        .expect("spawn echo server");
    println!("echo server listening on {address}");

    loop {
        let result = server
            .step(&mut io_loop)
            .expect("runtime should step echo server");
        if matches!(
            result,
            StepResult::ProcessedOne | StepResult::DroppedInvalid
        ) {
            continue;
        }

        io_loop.step().expect("io loop should step");
    }
}
