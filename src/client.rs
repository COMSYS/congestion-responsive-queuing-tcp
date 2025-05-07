use crate::{stats::GoodputRow, tcp_info::process_tcp_info, STATS_PRINT_INTERVAL};
use chrono::prelude::*;
use csv::Writer;
use failure::{Fail, Fallible, ResultExt};
use futures::{prelude::*, stream};
use kib;
use libc::{setsockopt, socklen_t, IPPROTO_TCP, TCP_CONGESTION};
use log::{debug, error, info};
use std::{
    io::{self, ErrorKind},
    net::SocketAddr,
    os::unix::io::AsRawFd,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{self,
            io::{AsyncReadExt,AsyncWriteExt},
            net::{TcpStream,TcpSocket},
            task};

pub async fn send_to(
    addr: SocketAddr,
    local_addr: Option<SocketAddr>,
    stats_folder: &Path,
    stats_record_interval: Duration,
    test_duration: Duration,
    number_of_flows: u16,
    cc: Option<&str>,
    bidirectional: u16,
    test_volume: u64,
    quit_bool: Arc<AtomicBool>,
) -> Fallible<()> {
    let stats_folder = Arc::new(stats_folder.to_owned());

    let connected_streams = stream::iter(0..number_of_flows)
        .map(|_| {
            connect_stream(addr.clone(), local_addr.clone())
                .map_err(|e| e.context("failed connecting stream"))
                .and_then(|stream| {
                    async {
                        let peer_addr = stream
                            .peer_addr()
                            .context("failed querying peer addr")?;
                        let local_addr = stream
                            .local_addr()
                            .context("failed querying local addr")?;

                        Ok((stream, peer_addr, local_addr))
                    }
                })
        })
        .buffer_unordered(64)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    if let Some(cc) = cc {
        for (stream, _, _) in connected_streams.iter() {
            set_congestion_control(stream, cc.trim())?;
        }
    }

    let handles = connected_streams
        .into_iter()
        .map(|(stream, remote_addr, local_addr)| {
            let tcp_info_handle = tokio::spawn(process_tcp_info(
                stream.as_raw_fd(),
                local_addr,
                remote_addr,
                stats_record_interval,
                stats_folder.clone(),
                quit_bool.clone(),
            ));
            let stream_handle = tokio::spawn(process_stream(
                stream,
                local_addr,
                remote_addr,
                test_duration,
                stats_record_interval,
                stats_folder.clone(),
                bidirectional,
                test_volume,
                quit_bool.clone(),
            ));

            (tcp_info_handle, stream_handle)
        })
        .collect::<Vec<_>>();

    for (tcp_info_handle, stream_handle) in handles {
        tcp_info_handle
            .await
            .expect("failed to spawn tcp info")
            .expect("failed to process TCP info");
        stream_handle
            .await
            .expect("failed to spawn stream")
            .expect("failed to process TCP stream");
    }

    Ok(())
}

async fn connect_stream(
    addr: SocketAddr,
    local_addr: Option<SocketAddr>,
) -> Fallible<TcpStream> {
    if let Some(local_addr) = local_addr {

        let socket = TcpSocket::new_v4()?;
        socket.bind(local_addr)?;

        let stream = socket.connect(addr)
            .await
            .context("failed connecting std stream")?;
        return Ok(stream);
    }

    let stream = TcpStream::connect(addr)
        .await
        .context("failed connecting tokio stream")?;
    Ok(stream)
}

fn set_congestion_control(s: &TcpStream, cc: &str) -> Fallible<()> {
    let fd = s.as_raw_fd();
    let res = unsafe {
        setsockopt(
            fd,
            IPPROTO_TCP,
            TCP_CONGESTION,
            cc.as_ptr() as *const core::ffi::c_void,
            cc.len() as socklen_t,
        )
    };

    if res < 0 {
        let err = io::Error::last_os_error()
            .context("failed setting congestion control algorithm")
            .into();
        return Err(err);
    }

    Ok(())
}

async fn process_stream(
    mut stream: TcpStream,
    _local_addr: SocketAddr,
    remote_addr: SocketAddr,
    test_duration: Duration,
    stats_record_interval: Duration,
    stats_folder: Arc<PathBuf>,
    bidirectional: u16,
    test_volume: u64,
    quit_bool: Arc<AtomicBool>,
) -> Fallible<()> {
    let csv_file_path = stats_folder.join(format!("recv-{}.csv", remote_addr));

    info!("accepting connection from {}", remote_addr);

    let mut csv = task::block_in_place(|| Writer::from_path(csv_file_path))?;

    let mut buf = [0u8; 1024 * 128];
    let eof = [0u8, 1u8, 2u8, 3u8, 4u8, 5u8];
    let mut read = 0u64;
    let mut total_read = 0u64;
    let test_start = Instant::now();
    let mut stats_print_instant = Instant::now();
    let mut stats_record_instant = Instant::now();
    let mut keep_receiving = true;
    let mut shutdown_connection = false;

    while !quit_bool.load(Ordering::SeqCst) {

        if keep_receiving {
            match stream.read(&mut buf).await {
                Ok(0) => {
                    debug!("read 0, bye bye");
                    quit_bool.store(true, Ordering::SeqCst);
                }
                Ok(r) => {

                    read += r as u64;
                    total_read += r as u64;

                    if r == 6 {
                        if buf[0] == 0 && buf[1] == 1 && buf[2] == 2 && buf[3] == 3 && buf[4] == 4 && buf[5] == 5 {
                            println!("Received EOF");
                            keep_receiving = false;
                        }
                    }

                    if bidirectional == 1 {
                        let half_r = r/2 as usize;

                        match stream.write_all(&buf[0..half_r]).await {
                            Ok(()) => {
                            },
                            Err(_e) => {
                                error!("Writing stops.");
                            }
                        }
                    }

                },
                Err(e)
                    if e.kind() == ErrorKind::UnexpectedEof
                        || e.kind() == ErrorKind::NotConnected =>
                {
                    debug!("Connection reset.");
                    quit_bool.store(true, Ordering::SeqCst);
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => continue,
                Err(e)
                    if e.kind() == ErrorKind::ConnectionReset
                        || e.kind() == ErrorKind::BrokenPipe
                        || e.kind() == ErrorKind::NotConnected
                        || e.kind() == ErrorKind::TimedOut =>
                {
                    debug!("Connection closed.");
                }
                Err(e) => {
                    error!("failed receiving data from {}: {:?}", remote_addr, e);
                    return Err(e.context("failed receiving data").into());
                }
            }
        }

        let now = Instant::now();
        let execution_time = now - test_start;

        if !keep_receiving {
            debug!("Other side requested shutdown.");
            println!("Other side requested shutdown. Quit now.");
            println!("Download complete. Time in milliseconds: {:?}", execution_time.as_secs() * 1000 + (execution_time.subsec_millis() as u64));
            shutdown_connection = true;
        }


        if total_read >= test_volume{
            debug!("Volume received.");
            println!("Volume received. Quit now.");
            println!("Download complete. Time in milliseconds: {:?}", execution_time.as_secs() * 1000 + (execution_time.subsec_millis() as u64));
            shutdown_connection = true;
        }

        if shutdown_connection {
            match stream.write_all(&eof).await {
                Ok(()) => {
                    println!("Writing worked");
                }
                Err(_e) => {
                    println!("Writing did not work. Assume its closed");
                }
            }
            quit_bool.store(true, Ordering::SeqCst);
            let _ = AsyncWriteExt::shutdown(&mut stream).await;
        }

        if (now - stats_record_instant) >= stats_record_interval {
            let rate = read as f64 / (now - stats_record_instant).as_secs_f64();

            if (now - stats_print_instant) >= STATS_PRINT_INTERVAL {
                info!(
                    "receiving from {} at {}ps",
                    remote_addr,
                    kib::format(rate as u128)
                );
                stats_print_instant = now;
            }

            task::block_in_place(|| {
                let row = GoodputRow {
                    goodput: rate as u64,
                    timestamp: Utc::now(),
                    total: total_read,
                };
                csv.serialize(row)
            })?;

            read = 0;
            stats_record_instant = now;
        }
    }

    task::block_in_place(move || csv.flush()).expect("failed flushing CSV writer");

    Ok(())
}
