use std::collections::HashMap;
use std::mem::{size_of, size_of_val};
use tokio::io::{ AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use std::io::{Error, ErrorKind, Result, Write};
use byteorder::{BigEndian, WriteBytesExt};

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

#[derive(Debug)]
struct ApiKeyAndVersion {
    api_key: i16,
    api_version: i16
}

const METADATA_API_KEY: i16 = 3;

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

    let mut requests: HashMap<i32, ApiKeyAndVersion> = HashMap::new();

    loop {
        let r = tokio::select! {
            request_size_res = client_rd_buf.read_i32() => {
                process_request(
                    &mut client_rd_buf, &mut broker_wr_buf,
                    request_size_res, &mut requests
                ).await
            }

            response_size_res = broker_rd_buf.read_i32() => {
                process_response(
                    &mut client_wr_buf, &mut broker_rd_buf,
                    response_size_res, &mut requests
                ).await
            }
        };

        match r {
            Ok(()) => {},

            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
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
                         request_size_res: Result<i32>,
                         requests: &mut HashMap<i32, ApiKeyAndVersion>) -> Result<()> {
    let request_size = request_size_res?;
    let api_key = client_rd_buf.read_i16().await?;
    let api_version = client_rd_buf.read_i16().await?;
    let correlation_id = client_rd_buf.read_i32().await?;

    println!("Request API key: {}, version: {}, correlation ID: {}",
             api_key, api_version, correlation_id);

    let buf_size = request_size as usize - size_of::<i16>() - size_of::<i16>() - size_of::<i32>();
    let mut buf = vec![0; buf_size];
    client_rd_buf.read_exact(&mut buf).await?;

    broker_wr_buf.write_i32(request_size).await?;
    broker_wr_buf.write_i16(api_key).await?;
    broker_wr_buf.write_i16(api_version).await?;
    broker_wr_buf.write_i32(correlation_id).await?;
    broker_wr_buf.write(&buf).await?;
    broker_wr_buf.flush().await?;

    requests.insert(correlation_id, ApiKeyAndVersion { api_key, api_version });

    println!("Request proxied");
    Ok(())
}

async fn process_response(client_wr_buf: &mut BufWriter<WriteHalf<'_>>,
                          broker_rd_buf: &mut BufReader<ReadHalf<'_>>,
                          response_size_res: Result<i32>,
                          requests: &mut HashMap<i32, ApiKeyAndVersion>) -> Result<()> {
    let response_size = response_size_res?;
    let mut buf_size = response_size as usize;

    let correlation_id = broker_rd_buf.read_i32().await?;
    buf_size -= size_of_val(&correlation_id);

    let (api_key, api_version) = match requests.remove(&correlation_id) {
        Some(ApiKeyAndVersion{api_key, api_version}) => (api_key, api_version),
        None => {
            return Err(Error::new(ErrorKind::InvalidData, format!("correlation ID {} not found", correlation_id)))
        }
    };

    println!("Response API key: {}, version: {}, correlation ID: {}",
             api_key, api_version, correlation_id);

    client_wr_buf.write_i32(response_size).await?;
    client_wr_buf.write_i32(correlation_id).await?;
    match api_key {
        METADATA_API_KEY => {
            process_metadata_response(client_wr_buf, broker_rd_buf, api_version, buf_size).await?;
        }

        _ => {
            let mut buf = vec![0; buf_size];
            broker_rd_buf.read_exact(&mut buf).await?;
            client_wr_buf.write(&buf).await?;
            client_wr_buf.flush().await?;
        }
    }

    println!("Response proxied\n");
    Ok(())
}

async fn process_metadata_response(client_wr_buf: &mut BufWriter<WriteHalf<'_>>,
                                   broker_rd_buf: &mut BufReader<ReadHalf<'_>>,
                                   api_version: i16,
                                   mut rest_size: usize) -> Result<()> {
    // TODO proper reuse, etc.
    let mut output_buf: Vec<u8> = Vec::with_capacity(rest_size);

    if api_version == 4 {
        // throttle_time_ms
        let (_, bytes_passed) = pass_i32(broker_rd_buf, &mut output_buf).await?;
        rest_size -= bytes_passed;

        // brokers array size
        let (broker_array_size, bytes_passed) = pass_i32(broker_rd_buf, &mut output_buf).await?;
        rest_size -= bytes_passed;

        for i in 0..broker_array_size {
            // node_id
            let (_, bytes_passed) = pass_i32(broker_rd_buf, &mut output_buf).await?;
            rest_size -= bytes_passed;

            // host
            rest_size -= pass_string(broker_rd_buf, &mut output_buf).await?;

            // port
            let (_, bytes_passed) = pass_i32(broker_rd_buf, &mut output_buf).await?;
            rest_size -= bytes_passed;

            // rack
            rest_size -= pass_nullable_string(broker_rd_buf, &mut output_buf).await?;
        }

        client_wr_buf.write(&output_buf).await?;

        let mut buf = vec![0; rest_size];
        broker_rd_buf.read_exact(&mut buf).await?;
        client_wr_buf.write(&buf).await?;
        client_wr_buf.flush().await?;
    } else {
        panic!("Not implemented")
    }

    Ok(())
}

async fn pass_i16(rd: &mut BufReader<ReadHalf<'_>>, buf: &mut Vec<u8>) -> Result<(i16, usize)> {
    let val = rd.read_i16().await?;
    let size = size_of_val(&val);
    WriteBytesExt::write_i16::<BigEndian>(buf, val)?;
    Ok((val, size))
}

async fn pass_i32(rd: &mut BufReader<ReadHalf<'_>>, buf: &mut Vec<u8>) -> Result<(i32, usize)> {
    let val = rd.read_i32().await?;
    let size = size_of_val(&val);
    WriteBytesExt::write_i32::<BigEndian>(buf, val)?;
    Ok((val, size))
}

async fn pass_string(rd: &mut BufReader<ReadHalf<'_>>, buf: &mut Vec<u8>) -> Result<usize> {
    let mut size: usize = 0;

    let (str_len, bytes_passed) = pass_i16(rd, buf).await?;
    size += bytes_passed;

    let mut str_buf = vec![0; str_len as usize];
    rd.read_exact(&mut str_buf).await?;

    let host = String::from_utf8(str_buf.clone()).unwrap();
    println!("HOST: {}", host);

    std::io::Write::write(buf, &str_buf)?;
    size += str_len as usize;

    Ok(size)
}

async fn pass_nullable_string(rd: &mut BufReader<ReadHalf<'_>>, buf: &mut Vec<u8>) -> Result<usize> {
    let mut size: usize = 0;

    let (str_len, bytes_passed) = pass_i16(rd, buf).await?;
    size += bytes_passed;

    if str_len >= 0 {
        let mut str_buf = vec![0; str_len as usize];
        rd.read_exact(&mut str_buf).await?;
        std::io::Write::write(buf, &str_buf)?;
        size += str_len as usize;
    }

    Ok(size)
}
