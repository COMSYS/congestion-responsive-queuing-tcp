use custom_tcp_sys::tcp_info;
use chrono::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TcpInfoRow {
    pub drop_count: u32,
    pub retrans: u32,
    pub retransmits: u8,
    pub total_retransmits: u32,
    pub rtt_us: u32,
    pub rtt_us_var: u32,
    pub send_congestion_window: u32,
    pub send_slow_start_threshold: u32,
    #[serde(rename = "timestamp_utc_ns", with = "chrono::serde::ts_nanoseconds")]
    pub timestamp: DateTime<Utc>,
}

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct GoodputRow {
    pub goodput: u64,
    #[serde(rename = "timestamp_utc_ns", with = "chrono::serde::ts_nanoseconds")]
    pub timestamp: DateTime<Utc>,
    pub total: u64,
}

impl From<tcp_info> for TcpInfoRow {
    fn from(info: tcp_info) -> Self {
        TcpInfoRow {
            drop_count: info.tcpi_lost,
            retrans: info.tcpi_retrans,
            retransmits: info.tcpi_retransmits,
            total_retransmits: info.tcpi_total_retrans,
            rtt_us: info.tcpi_rtt,
            rtt_us_var: info.tcpi_rttvar,
            send_congestion_window: info.tcpi_snd_cwnd,
            send_slow_start_threshold: info.tcpi_snd_ssthresh,
            timestamp: Utc::now(),
        }
    }
}
