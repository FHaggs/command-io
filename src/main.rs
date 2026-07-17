mod arena;
mod completion;
mod effects;
mod io;
mod runtime;

use completion::CompletionHandle;
use effects::{RuntimeMessage, TurnEffects};
use io::{FakeIoDriver, IoResult, Operation, RawFdLike, RecvOp, SendOp};
use runtime::{IoContext, IoLoop, Isolate, RuntimeError, Server};

enum Phase {
    Reading,
    Writing,
    Closing,
}

struct EchoConnection {
    fd: RawFdLike,
    phase: Phase,
    recv: CompletionHandle,
    send: CompletionHandle,
}

impl EchoConnection {
    fn new(fd: RawFdLike) -> Self {
        Self {
            fd,
            phase: Phase::Reading,
            recv: CompletionHandle::INVALID,
            send: CompletionHandle::INVALID,
        }
    }

    fn submit_recv(&self, effects: &mut TurnEffects) -> Result<(), RuntimeError> {
        effects.submit(
            self.recv,
            Operation::Recv(RecvOp {
                fd: self.fd,
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
                fd: self.fd,
                buf,
                flags: 0,
            }),
        )?;
        Ok(())
    }
}

impl Isolate for EchoConnection {
    fn handle(
        &mut self,
        msg: RuntimeMessage,
        io: &mut IoContext<'_>,
        effects: &mut TurnEffects,
    ) -> Result<(), RuntimeError> {
        match msg {
            RuntimeMessage::Init => {
                self.recv = io.acquire().expect("completion arena full");
                self.send = io.acquire().expect("completion arena full");
                self.submit_recv(effects)?;
            }
            RuntimeMessage::IoCompleted(completion) if completion == self.recv => {
                match io.take_result(completion) {
                    Some(IoResult::Recv(Ok(bytes))) if bytes.is_empty() => {
                        self.phase = Phase::Closing;
                        effects.destroy_self()?;
                    }
                    Some(IoResult::Recv(Ok(bytes))) => {
                        self.phase = Phase::Writing;
                        self.submit_send(bytes, effects)?;
                    }
                    Some(IoResult::Recv(Err(_))) | Some(IoResult::Cancelled) => {
                        self.phase = Phase::Closing;
                        effects.destroy_self()?;
                    }
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
                    Some(IoResult::Send(Err(_))) | Some(IoResult::Cancelled) => {
                        self.phase = Phase::Closing;
                        effects.destroy_self()?;
                    }
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
        effects.cancel(self.recv)?;
        effects.cancel(self.send)?;
        Ok(())
    }
}

fn main() {
    let mut server = Server::new(16, 64, 8);
    let mut io_loop = IoLoop::new(FakeIoDriver::new(64), 32);

    server
        .spawn(EchoConnection::new(7))
        .expect("spawn echo connection");
    let processed = server
        .run_until_idle(&mut io_loop)
        .expect("runtime should drain echo connection");
    println!("processed {processed} isolate turns");
}
