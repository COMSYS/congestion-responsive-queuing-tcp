use crate::stats::TcpInfoRow;
use custom_tcp_sys::tcp_info;
use csv::Writer;
use failure::{Fallible, ResultExt};
use libc::{getsockopt, SOL_TCP, TCP_INFO};
use std::{
    io,
    mem::{self, MaybeUninit},
    net::SocketAddr,
    os::unix::io::RawFd,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{task};

pub async fn process_tcp_info(
    sock: RawFd,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    record_interval: Duration,
    stats_folder: Arc<PathBuf>,
    quit_bool: Arc<AtomicBool>,
) -> Fallible<()> {
    let path =
        stats_folder.join(format!("tcp-info-{}-{}.csv", remote_addr, local_addr));
    let mut writer = task::block_in_place(|| Writer::from_path(path))?;

    while !quit_bool.load(Ordering::SeqCst) {
        let _ = tokio::time::sleep(record_interval).await;

        task::block_in_place(|| {
            match get_tcp_info(sock).context("failed querying TCP info")? {
                Some(info) => writer
                    .serialize(TcpInfoRow::from(info))
                    .context("failed serializing CSV"),
                _ => Ok(()),
            }
        })?;
    }

    Ok(())
}

fn get_tcp_info(sock: RawFd) -> io::Result<Option<tcp_info>> {
    unsafe {
        let mut info = MaybeUninit::<tcp_info>::zeroed();
        let mut size = mem::size_of::<tcp_info>() as u32;

        let res = getsockopt(
            sock,
            SOL_TCP,
            TCP_INFO,
            info.as_mut_ptr() as *mut core::ffi::c_void,
            &mut size as *mut u32,
        );

        if res < 0 {
            let err = io::Error::last_os_error();

            match err.raw_os_error() {
                Some(9) => Ok(None),
                _ => Err(err),
            }
        } else if size as usize != mem::size_of::<tcp_info>() {
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "invalid size: {}, should be {}",
                    size,
                    mem::size_of::<tcp_info>()
                ),
            ))
        } else {
            Ok(Some(info.assume_init()))
        }
    }
}
