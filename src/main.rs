use std::io::ErrorKind::UnexpectedEof;
use std::mem::size_of;
use tokio::io::{ AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use std::io::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:10000").await.unwrap();
    loop {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            process(socket).await;
        });
    }
}


async fn process(mut client_socket: TcpStream) {
    client_socket.set_nodelay(true).unwrap();
    // client_socket.readable().await.unwrap();
    // client_socket.writable().await.unwrap();
    let (client_rd, client_wr) = client_socket.split();
    let mut client_rd_buf = BufReader::new(client_rd);
    let mut client_wr_buf = BufWriter::new(client_wr);

    let mut broker_socket = TcpStream::connect("127.0.0.1:9092").await.unwrap();
    // broker_socket.readable().await.unwrap();
    // broker_socket.writable().await.unwrap();
    let (broker_rd, broker_wr) = broker_socket.split();
    let mut broker_rd_buf = BufReader::new(broker_rd);
    let mut broker_wr_buf = BufWriter::new(broker_wr);

    loop {
        let r = tokio::select! {
            request_size_res = client_rd_buf.read_i32() => {
                process_request(&mut client_rd_buf, &mut broker_wr_buf, request_size_res).await
            }

            response_size_res = broker_rd_buf.read_i32() => {
                process_response(&mut client_wr_buf, &mut broker_rd_buf, response_size_res).await
            }
        };

        match r {
            Ok(()) => {},

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
    }
}

async fn process_request(client_rd_buf: &mut BufReader<ReadHalf<'_>>,
                         broker_wr_buf: &mut BufWriter<WriteHalf<'_>>,
                         request_size_res: Result<i32>) -> Result<()> {
    let request_size = request_size_res?;
    let request_api_key = client_rd_buf.read_i16().await?;
    let request_api_version = client_rd_buf.read_i16().await?;
    let correlation_id = client_rd_buf.read_i32().await?;

    println!("Request API key: {}, version: {}, correlation ID: {}",
             request_api_key, request_api_version, correlation_id);

    let buf_size = request_size as usize - size_of::<i16>() - size_of::<i16>() - size_of::<i32>();
    let mut buf = vec![0; buf_size];
    client_rd_buf.read_exact(&mut buf).await?;

    broker_wr_buf.write_i32(request_size).await?;
    broker_wr_buf.write_i16(request_api_key).await?;
    broker_wr_buf.write_i16(request_api_version).await?;
    broker_wr_buf.write_i32(correlation_id).await?;
    broker_wr_buf.write(&buf).await?;
    broker_wr_buf.flush().await?;

    println!("Request proxied");
    Ok(())
}

async fn process_response(client_wr_buf: &mut BufWriter<WriteHalf<'_>>,
                          broker_rd_buf: &mut BufReader<ReadHalf<'_>>,
                          response_size_res: Result<i32>) -> Result<()> {
    let response_size = response_size_res?;
    let correlation_id = broker_rd_buf.read_i32().await?;

    println!("Response correlation ID: {}",
             correlation_id);

    let buf_size = response_size as usize - size_of::<i32>();
    let mut buf = vec![0; buf_size];
    broker_rd_buf.read_exact(&mut buf).await?;
    client_wr_buf.write_i32(response_size).await?;
    client_wr_buf.write_i32(correlation_id).await?;
    client_wr_buf.write(&buf).await?;
    client_wr_buf.flush().await?;

    println!("Response proxied");
    Ok(())
}
