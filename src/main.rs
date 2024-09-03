use dora_node_api::arrow::datatypes::ToByteSlice;
use dora_node_api::{ArrowData, DoraNode, Event};
use std::io::Write;
use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

fn main() -> eyre::Result<()> {
    let (sx, rx) = mpsc::channel();

    let _unused = thread::spawn(move || {
        forward_server(rx);
    });

    let (_, mut events) = DoraNode::init_from_env()?;

    loop {
        let event = match events.recv() {
            Some(input) => input,
            None => break,
        };

        match event {
            Event::Input { data, .. } => {
                let _ = sx.send(data);
            }
            Event::Stop => println!("Received manual stop"),
            other => eprintln!("Received unexpected input: {other:?}"),
        }
    }
    // thread::sleep(Duration::from_secs(5));

    eyre::Ok(())
}

fn forward_server(receiver: mpsc::Receiver<ArrowData>) {
    let listener = TcpListener::bind("127.0.0.1:12345").unwrap();
    for stream in listener.incoming() {
        let mut stream = stream.unwrap();
        loop {
            let data = receiver.recv().unwrap();
            stream.write((&data).try_into().unwrap()).unwrap();
        }
    }
}
