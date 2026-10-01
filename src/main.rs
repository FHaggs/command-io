mod completion;
mod io;
mod reader;

use std::error::Error;
use std::fs::File;

use completion::OwnerHandle;
use io::IoContext;
use reader::{Reader, ReaderState};

/// Owns the readers and routes ready completions to them. Owners are indexed
/// by `OwnerHandle.0`.
struct Server {
    readers: Vec<Reader>,
}

impl Server {
    fn step(&mut self, io: &mut IoContext) -> Result<(), Box<dyn Error>> {
        for reader in &mut self.readers {
            reader.step(io)?;
        }
        while let Some(completion) = io.pop_ready() {
            let owner = io.owner(completion)?;
            self.readers[owner.0 as usize].on_io_completed(io, completion)?;
        }
        Ok(())
    }

    fn is_finished(&self) -> bool {
        self.readers.iter().all(Reader::is_finished)
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let paths: Vec<String> = std::env::args().skip(1).collect();
    let paths = if paths.is_empty() {
        vec!["Cargo.toml".to_owned(), "README.md".to_owned()]
    } else {
        paths
    };

    let mut io = IoContext::new(paths.len() as u32)?;
    let mut server = Server {
        readers: Vec::with_capacity(paths.len()),
    };
    for (id, path) in paths.iter().enumerate() {
        let reader = Reader::new(OwnerHandle(id as u32), &mut io, File::open(path)?, 4096)?;
        server.readers.push(reader);
    }

    while !server.is_finished() {
        server.step(&mut io)?;
        io.step()?;
    }

    for (path, reader) in paths.iter().zip(&server.readers) {
        match reader.state() {
            ReaderState::Done { bytes } => {
                let data = reader.data(&io).unwrap_or_default();
                let first_line = String::from_utf8_lossy(data);
                let first_line = first_line.lines().next().unwrap_or("");
                println!("{path}: read {bytes} bytes, first line: {first_line:?}");
            }
            ReaderState::Failed(err) => println!("{path}: failed: {err}"),
            state => unreachable!("unfinished reader: {state:?}"),
        }
    }
    for reader in server.readers {
        reader.close(&mut io)?;
    }
    Ok(())
}
