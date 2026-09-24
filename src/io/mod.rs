use io_uring::squeue::Entry;
use io_uring::{IoUring, opcode, types::Fd};
use std::fs::File;
use std::os::unix::io::AsRawFd;

enum CompletionState {
    Pending,
    Completed,
    Failed,
}

struct Completion {
    op: Entry,
    state: CompletionState,
}

struct IoContext {
    engine: IoEngine,
    queued: Vec<Completion>,
}

struct IoEngine {
    ring: IoUring,
}

impl IoEngine {
    fn new(entries: u32) -> std::io::Result<Self> {
        let ring = IoUring::new(entries)?;
        Ok(Self { ring })
    }
    fn submit(&mut self, entry_op:Entry) {
        unsafe {
            self.ring.submission().push(&entry_op).expect("submission queue full");
        }
        self.ring.submit().expect("failed to submit");
    }

    fn pool_one(&mut self) -> Option<std::io::Result<usize>> {
        for cqe in self.ring.completion() {
            let res = cqe.result();
            if res < 0 {
                return Some(Err(std::io::Error::from_raw_os_error(-res)));
            } else {
                return Some(Ok(res as usize));
            }
        }
        None
    }

}

/// Reads a file using io_uring and handles errors.
fn read_file_chunk(path: &str, buf: &mut [u8], io_context: &mut IoEngine) -> std::io::Result<usize> {
    // Open the file and get the raw file descriptor.
    let file = File::open(path)?;
    // let mut ring = IoUring::new(8)?;
    // let mut sq = ring.submission();

    // Allocate a buffer for the read.
    // This buffer must live until the kernel finishes writing.
    // let mut buf = vec![0u8; 4096];

    // Create the read operation.
    // We pass the raw fd and a pointer to the buffer.
    let op = opcode::Read::new(
        Fd(file.as_raw_fd()),
        buf.as_mut_ptr(),
        buf.len() as u32,
    );

    let entry = op.build();
    unsafe {
        io_context.submit(entry);
    }

    loop {
        if let Some(res) = io_context.pool_one() {
            return res;
        }
    }

}


#[cfg(test)]
mod tests {
    use std::println;

use super::*;

    #[test]
    fn test_read_file_chunk() {
        let path = "Cargo.toml";
        let mut buf = vec![0u8; 4096];
        let mut io_context = IoEngine::new(8).expect("Failed to create IoContext");
        let result = read_file_chunk(path, &mut buf, &mut io_context).expect("msg").clone();
        println!("Buf: {}", String::from_utf8_lossy(&buf[..result]));
    }
}
