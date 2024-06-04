use std::hint::black_box;
use std::io::ErrorKind::UnexpectedEof;
use std::mem::size_of;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter, stdout};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> tokio::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:10000").await.unwrap();
    loop {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            process(socket).await;
        });
    }

    Ok(())
}


async fn process(mut client_socket: TcpStream) {
    client_socket.set_nodelay(true).unwrap();
    // client_socket.readable().await.unwrap();
    // client_socket.writable().await.unwrap();
    let (mut client_rd, mut client_wr) = client_socket.split();
    let mut client_rd_buf = BufReader::new(client_rd);
    let mut client_wr_buf = BufWriter::new(client_wr);

    let mut broker_socket = TcpStream::connect("127.0.0.1:9092").await.unwrap();
    // broker_socket.readable().await.unwrap();
    // broker_socket.writable().await.unwrap();
    let (mut broker_rd, mut broker_wr) = broker_socket.split();
    let mut broker_rd_buf = BufReader::new(broker_rd);
    let mut broker_wr_buf = BufWriter::new(broker_wr);

    loop {
        let request_size = match client_rd_buf.read_i32().await {
            Ok(s) => s,

            Err(e) if e.kind() == UnexpectedEof => {
                client_socket.shutdown().await.unwrap();
                broker_socket.shutdown().await.unwrap();
                return;
            }

            Err(e) => {
                println!("Unexpected error {}", e);
                client_socket.shutdown().await.unwrap();
                broker_socket.shutdown().await.unwrap();
                return;
            }
        };

        let request_api_key = client_rd_buf.read_i16().await.unwrap();
        let request_api_version = client_rd_buf.read_i16().await.unwrap();
        let correlation_id = client_rd_buf.read_i32().await.unwrap();

        println!("Request API key: {}, version: {}, correlation ID: {}",
                 request_api_key, request_api_version, correlation_id);

        let buf_size = request_size as usize - size_of::<i16>() - size_of::<i16>() - size_of::<i32>();
        let mut buf = vec![0; buf_size];
        client_rd_buf.read_exact(&mut buf).await.unwrap();

        broker_wr_buf.write_i32(request_size).await.unwrap();
        broker_wr_buf.write_i16(request_api_key).await.unwrap();
        broker_wr_buf.write_i16(request_api_version).await.unwrap();
        broker_wr_buf.write_i32(correlation_id).await.unwrap();
        broker_wr_buf.write(&buf).await.unwrap();
        broker_wr_buf.flush().await.unwrap();

        println!("Request proxied");

        let response_size = broker_rd_buf.read_i32().await.unwrap();
        // let correlation_id = client_rd_buf.read_i32().await.unwrap();

        // println!("Response correlation ID: {}",
        //          correlation_id);

        // let buf_size = response_size as usize - size_of::<i32>();
        let buf_size = response_size as usize;
        let mut buf = vec![0; buf_size];
        broker_rd_buf.read_exact(&mut buf).await.unwrap();
        client_wr_buf.write_i32(response_size).await.unwrap();
        // client_wr_buf.write_i32(correlation_id).await.unwrap();
        client_wr_buf.write(&buf).await.unwrap();
        client_wr_buf.flush().await.unwrap();

        println!("Response proxied")
    }
}

async fn process_from_client_to_broker() {

}

async fn process_from_broker_to_client() {

}
