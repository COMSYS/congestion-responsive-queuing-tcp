use crate::{stats::GoodputRow, tcp_info::process_tcp_info, STATS_PRINT_INTERVAL};
use chrono::prelude::*;
use csv::Writer;
use failure::{Fail, Fallible, ResultExt};
use libc::{setsockopt, socklen_t, IPPROTO_TCP, TCP_CONGESTION};
use log::{debug, error, info};
use std::{
    io::{self,ErrorKind},
    net::SocketAddr,
    os::unix::io::AsRawFd,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    self,
    io::{AsyncWriteExt,Interest},
    net::{TcpListener, TcpStream},
    task,
};

const SEND_DATA: [u8; 1024 * 1024] = [0; 1024 * 1024];

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

pub async fn accept_from(
    listen_addr: SocketAddr,
    stats_folder: &Path,
    stats_record_interval: Duration,
    test_duration: Duration,
    number_of_flows: u16,
    cc: Option<&str>,
    bidirectional: u16,
    test_volume: u64,
    quit_bool: Arc<AtomicBool>,
) -> Fallible<()> {
    let listener = TcpListener::bind(listen_addr).await?;
    info!(
        "listening on {}, accepting {} streams",
        listener.local_addr()?,
        number_of_flows
    );

    let stats_folder = Arc::new(stats_folder.to_owned());

    let (current_socket, remote_addr) = listener.accept().await?;

    if let Some(cc) = cc {
        set_congestion_control(&current_socket, cc.trim())?;
    }

    let mut handles = Vec::new();

    let tcp_info_handle = tokio::spawn(process_tcp_info(
        current_socket.as_raw_fd(),
        current_socket.local_addr().context("failed to query local addr")?,
        remote_addr,
        stats_record_interval,
        stats_folder.clone(),
        quit_bool.clone(),
    ));
    let stream_handle = tokio::spawn(process_stream(
        current_socket,
        listen_addr,
        remote_addr,
        test_duration,
        stats_record_interval,
        stats_folder.clone(),
        bidirectional,
        test_volume,
        quit_bool.clone(),
    ));

    handles.push(tcp_info_handle);
    handles.push(stream_handle);
    

    for handle in handles {
        handle
            .await
            .context("failed to spawn future")?
            .context("failed to process receiver side")?;
    }

    Ok(())
}

async fn process_stream(
    mut stream: TcpStream,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    test_duration: Duration,
    stats_record_interval: Duration,
    stats_folder: Arc<PathBuf>,
    _bidirectional: u16,
    test_volume: u64,
    quit_bool: Arc<AtomicBool>,
) -> Fallible<()> {
    let csv_file_path = stats_folder.join(format!("send-{}.csv", local_addr));

    debug!("opening connection to {}", remote_addr);

    let mut csv = task::block_in_place(|| Writer::from_path(csv_file_path))?;

    let mut buf = [0u8; 1024 * 128];
    let eof = [0u8, 1u8, 2u8, 3u8, 4u8, 5u8];
    let mut written = 0u64;
    let mut total_written = 0u64;
    let mut stats_print_instant = Instant::now();
    let mut stats_record_instant = Instant::now();
    let mut send_data = true;
    let mut stop_sent = false;
    let mut send_the_stop_signal = false;

    while !quit_bool.load(Ordering::SeqCst) {

        let ready = stream.ready(Interest::READABLE | Interest::WRITABLE).await?;
        //stream.writable().await?;

        if ready.is_writable() && send_data {
            match stream.write(&SEND_DATA).await {
                Ok(0) => info!("wrote 0 bytes to network"),
                Ok(n) => {
                    written += n as u64;
                    total_written += n as u64;
                },
                Err(e)
                    if e.kind() == ErrorKind::ConnectionReset
                        || e.kind() == ErrorKind::BrokenPipe
                        || e.kind() == ErrorKind::NotConnected
                        || e.kind() == ErrorKind::TimedOut =>
                {
                    debug!("Connection closed.");
                    quit_bool.store(true, Ordering::SeqCst);
                    break;
                },
                Err(e)
                    if e.kind() == ErrorKind::UnexpectedEof
                        || e.kind() == ErrorKind::NotConnected =>
                {
                    debug!("Connection reset.");
                    quit_bool.store(true, Ordering::SeqCst);
                    break;
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => 
                {
                }
                Err(e) => {
                    error!("Failed writing data: {:?}", e);
                    return Err(e.into());
                }
            }
        }

        if ready.is_readable(){
            match stream.try_read(&mut buf) {
                Ok(0) => {
                }
                Ok(n) => {
                    if n == 6 {
                        if buf[0] == 0 && buf[1] == 1 && buf[2] == 2 && buf[3] == 3 && buf[4] == 4 && buf[5] == 5 {
                            println!("Received EOF. Other side also ready to close. Close here.");
                            quit_bool.store(true, Ordering::SeqCst);
                            let _ = AsyncWriteExt::shutdown(&mut stream).await;
                            break;
                        }
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                }
                Err(e)
                    if e.kind() == ErrorKind::UnexpectedEof
                        || e.kind() == ErrorKind::NotConnected =>
                {
                    debug!("Connection reset.");
                    quit_bool.store(true, Ordering::SeqCst);
                    break;
                }
                Err(e)
                    if e.kind() == ErrorKind::ConnectionReset
                        || e.kind() == ErrorKind::BrokenPipe
                        || e.kind() == ErrorKind::NotConnected
                        || e.kind() == ErrorKind::TimedOut =>
                {
                    debug!("Connection closed.");
                    quit_bool.store(true, Ordering::SeqCst);
                    break;
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        } else {
            if stop_sent {

                match stream.try_read(&mut buf) {
                    Ok(0) => {
                        println!("Other side is closed.");
                        quit_bool.store(true, Ordering::SeqCst);
                        let _ = AsyncWriteExt::shutdown(&mut stream).await;
                    }
                    Ok(_n) => {
                        println!("Not yet closed");
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        //
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            }
        }

        let now = Instant::now();

        if total_written >= test_volume && !stop_sent{
            send_data = false;
            debug!("Volume transmitted.");
            println!("Volume transmitted. Quit now.");
            send_the_stop_signal = true;
        }

        if send_the_stop_signal {
            match stream.write_all(&eof).await {
                Ok(()) => {
                    println!("Send a stop signal.");
                    stop_sent = true;
                    let _ = tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(_e) => {
                    println!("Writing did not work. Assume its closed");
                    quit_bool.store(true, Ordering::SeqCst);
                    let _ = AsyncWriteExt::shutdown(&mut stream).await;
                }
            }
        }

        if (now - stats_record_instant) >= stats_record_interval {
            let rate = written as f64 / (now - stats_record_instant).as_secs_f64();

            if (now - stats_print_instant) >= STATS_PRINT_INTERVAL {
                debug!(
                    "sending to {} at {}ps",
                    remote_addr,
                    kib::format(rate as u128)
                );
                stats_print_instant = now;
            }

            task::block_in_place(|| {
                let row = GoodputRow {
                    goodput: rate as u64,
                    timestamp: Utc::now(),
                    total: total_written,
                };
                csv.serialize(row)
            })?;

            written = 0;
            stats_record_instant = now;
        }
    }

    task::block_in_place(move || csv.flush()).expect("failed flushing CSV writer");

    Ok(())
}
