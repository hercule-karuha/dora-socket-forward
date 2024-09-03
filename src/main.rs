use dora_node_api::{ArrowData, DoraNode, Event};
use std::borrow::Borrow;
use std::env;
use std::io::Write;
use std::net::TcpListener;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;

static CONNECTED: AtomicBool = AtomicBool::new(false);
fn main() -> eyre::Result<()> {
    dotenv::dotenv().expect("no .env");
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
                if CONNECTED.load(Ordering::Relaxed) {
                    let _unused = sx.send(data);
                }
            }
            Event::Stop => println!("Received manual stop"),
            other => eprintln!("Received unexpected input: {other:?}"),
        }
    }
    eyre::Ok(())
}

fn forward_server(receiver: mpsc::Receiver<ArrowData>) {
    let listener = TcpListener::bind(env::var("LISTEN_ADDR").unwrap()).unwrap();
    println!("server start");
    for stream in listener.incoming() {
        CONNECTED.store(true, Ordering::Relaxed);
        let mut stream = stream.unwrap();
        loop {
            let data = receiver.recv().unwrap();
            stream.write(data.borrow().try_into().unwrap()).unwrap();
        }
    }
}
