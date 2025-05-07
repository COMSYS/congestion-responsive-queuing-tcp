use chrono::Utc;
use clap::{
    app_from_crate, crate_authors, crate_description, crate_name, crate_version,
    AppSettings, Arg, SubCommand,
};
use ctrlc;
use env_logger::{self, Env};
use mkdirp::mkdirp;
use std::{
    error::Error,
    fs,
    net::{SocketAddr, ToSocketAddrs},
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio;

mod client;
mod server;
mod stats;
mod tcp_info;

const LOCAL_ADDR_ARG: &str = "LOCAL_ADDR";
const REMOTE_ADDR_ARG: &str = "REMOTE_ADDR";
const NO_FLOWS_ARG: &str = "NUMBER_OF_FLOWS";
const STATS_SAMPLING_RATE_ARG: &str = "SAMPLING_RATE";
const STATS_OUTPUT_FOLDER_ARG: &str = "STATS_FOLDER";
const TEST_DURATION_ARG: &str = "TEST_DURATION_SECS";
const TCP_CONGESTION_ARG: &str = "TCP_CONGESTION_CONTROL";
const TEST_VOLUME_ARG: &str = "TEST_VOLUME_MB";
const BIDIRECTIONAL_ARG: &str = "BIDIRECTIONAL";

const CLIENT_SUBCOMMAND: &str = "client";
const SERVER_SUBCOMMAND: &str = "server";

pub const STATS_PRINT_INTERVAL: Duration = Duration::from_secs(1);

fn validate_addr(input: String) -> Result<(), String> {
    input
        .to_socket_addrs()
        .map(|_| ())
        .map_err(|_| format!(
            "{} is not a valid socket address, also needs to include the port number",
            input,
        ))
}

fn validate_cc(input: String) -> Result<(), String> {
    let available =
        fs::read_to_string("/proc/sys/net/ipv4/tcp_available_congestion_control")
            .map_err(|_| {
                "failed to read available congestion control algorithms".to_owned()
            })?;
    if available.trim().split(" ").any(|value| value.trim() == input.trim()) {
        Ok(())
    } else {
        Err(format!(
            "'{}' is not supported on this system, please choose from: {}",
            input, available
        ))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    env_logger::from_env(Env::default().default_filter_or("info")).init();
    let stats_folder_name = Utc::now().format("./stats-%F+%T").to_string();

    let stats_sampling_rate_arg = Arg::with_name(STATS_SAMPLING_RATE_ARG)
        .short("r")
        .long("rate")
        .help("The interval (in microseconds) at which to record connection statistics")
        .takes_value(true)
        .required(true)
        .default_value("10000")
        .validator(|v| {
            v.parse::<u64>()
                .map(|_| ())
                .map_err(|_| format!("{} is not a valid interval", v))
        });
    let stats_output_folder_arg = Arg::with_name(STATS_OUTPUT_FOLDER_ARG)
        .short("o")
        .long("output")
        .help("The path to the folder to output the TCP statistics to")
        .takes_value(true)
        .required(true)
        .default_value(&stats_folder_name);
    let test_duration_arg = Arg::with_name(TEST_DURATION_ARG)
        .short("d")
        .long("duration")
        .help("The duration (in seconds) the tests should be run for.")
        .takes_value(true)
        .required(true)
        .default_value("300")
        .validator(|v| {
            v.parse::<u64>()
                .map(|_| ())
                .map_err(|_| format!("{} is not a valid duration", v))
        });
    let number_flows_arg = Arg::with_name(NO_FLOWS_ARG)
        .short("f")
        .long("flows")
        .help("The number of TCP flows to create.")
        .required(true)
        .default_value("1")
        .takes_value(true)
        .validator(|v| {
            v.parse::<u16>()
                .map(|_| ())
                .map_err(|_| format!("{} is not a valid number of flows", v))
        });
    let test_volume_arg = Arg::with_name(TEST_VOLUME_ARG)
        .short("v")
        .long("volume")
        .help("The volume of data that should be transmitted (in MB).")
        .takes_value(true)
        .required(true)
        .default_value("200")
        .validator(|v| {
            v.parse::<u64>()
                .map(|_| ())
                .map_err(|_| format!("{} is not a valid volume", v))
        });
    let bidirectional_arg = Arg::with_name(BIDIRECTIONAL_ARG)
        .short("b")
        .long("bidirectional")
        .help("Specifies if bidirectional traffic is to be used.")
        .takes_value(true)
        .required(true)
        .default_value("0")
        .validator(|v| {
            v.parse::<u64>()
                .map(|_| ())
                .map_err(|_| format!("{} is not a valid directionality", v))
        });


    let cc_arg = Arg::with_name(TCP_CONGESTION_ARG)
        .short("c")
        .long("congestion")
        .help("The congestion control algorithm to use.")
        .takes_value(true)
        .required(false)
        .validator(validate_cc);
    

    let matches = app_from_crate!()
        .setting(AppSettings::GlobalVersion)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(
            SubCommand::with_name(SERVER_SUBCOMMAND)
                .about("Set up the server")
                .arg(stats_sampling_rate_arg.clone())
                .arg(stats_output_folder_arg.clone())
                .arg(test_duration_arg.clone())
                .arg(number_flows_arg.clone())
                .arg(test_volume_arg.clone())
                .arg(bidirectional_arg.clone())
                .arg(cc_arg.clone())
                .arg(
                    Arg::with_name(LOCAL_ADDR_ARG)
                        .short("a")
                        .long("local_address")
                        .help("The local address to listen on (including port number).")
                        .default_value("0.0.0.0:1337")
                        .env(LOCAL_ADDR_ARG)
                        .takes_value(true)
                        .required(true)
//                        .index(1)
                        .validator(validate_addr),
                ),
        )
        .subcommand(
            SubCommand::with_name(CLIENT_SUBCOMMAND)
                .about("Set up the client")
                .arg(stats_sampling_rate_arg.clone())
                .arg(stats_output_folder_arg.clone())
                .arg(test_duration_arg.clone())
                .arg(number_flows_arg.clone())
                .arg(test_volume_arg.clone())
                .arg(bidirectional_arg.clone())
                .arg(cc_arg.clone())
                .arg(
                    Arg::with_name(REMOTE_ADDR_ARG)
                        .short("a")
                        .long("remote_address")
                        .help("The address of the client to connect to (including port number).")
//                        .index(1)
                        .takes_value(true)
                        .env(REMOTE_ADDR_ARG)
                        .required(true)
                        .validator(validate_addr),
                )
                .arg(
                    Arg::with_name(LOCAL_ADDR_ARG)
                        .short("l")
                        .long("local_address")
                        .help("The local address to bind the connecting socket to.")
                        .env(LOCAL_ADDR_ARG)
                        .takes_value(true)
                        .required(false)
                        .validator(validate_addr),
                )
        )
        .get_matches();

    let args_for_both_subcommands = matches
        .subcommand_matches(SERVER_SUBCOMMAND)
        .or(matches.subcommand_matches(CLIENT_SUBCOMMAND))
        .unwrap();

    let stats_interval = {
        let micros = args_for_both_subcommands
            .value_of(STATS_SAMPLING_RATE_ARG)
            .unwrap()
            .parse()
            .unwrap();
        Duration::from_micros(micros)
    };

    let stats_folder = args_for_both_subcommands
        .value_of(STATS_OUTPUT_FOLDER_ARG)
        .unwrap();
    let stats_folder = Path::new(stats_folder);
    mkdirp(stats_folder)?;

    let test_duration = {
        let secs = args_for_both_subcommands
            .value_of(TEST_DURATION_ARG)
            .unwrap()
            .parse()
            .unwrap();
        Duration::from_secs(secs)
    };

    let no_flows = args_for_both_subcommands
        .value_of(NO_FLOWS_ARG)
        .unwrap()
        .parse()
        .unwrap();

    let bidirectional = args_for_both_subcommands
        .value_of(BIDIRECTIONAL_ARG)
        .unwrap()
        .parse()
        .unwrap();

    let cc = args_for_both_subcommands
        .value_of(TCP_CONGESTION_ARG);

    if bidirectional != 0 && bidirectional != 1 {
        panic!("{} is no valid value for bidirectional.", bidirectional);
    }

    let mut test_volume = args_for_both_subcommands
        .value_of(TEST_VOLUME_ARG)
        .unwrap()
        .parse()
        .unwrap();
    test_volume = test_volume * 1000 * 1000;


    let quit_bool = Arc::new(AtomicBool::new(false));
    let quit_bool_ctrl_c = quit_bool.clone();
    ctrlc::set_handler(move || quit_bool_ctrl_c.store(true, Ordering::SeqCst))?;

    match matches.subcommand() {
        (SERVER_SUBCOMMAND, Some(args)) => {
            let addr = args
                .value_of(LOCAL_ADDR_ARG)
                .unwrap()
                .parse::<SocketAddr>()
                .expect("failed to resolve target address");
            server::accept_from(
                addr,
                stats_folder,
                stats_interval,
                test_duration,
                no_flows,
                cc,
                bidirectional,
                test_volume,
                quit_bool.clone(),
            )
            .await?;
        }
        (CLIENT_SUBCOMMAND, Some(args)) => {
            let addr = args
                .value_of(REMOTE_ADDR_ARG)
                .unwrap()
                .parse::<SocketAddr>()
                .expect("failed to resolve target address");
            let local_addr = args.value_of(LOCAL_ADDR_ARG).to_owned().map(|addr| {
                addr.parse::<SocketAddr>()
                    .expect("failed to resolve local address")
            });
            //let cc = args.value_of(TCP_CONGESTION_ARG);

            client::send_to(
                addr,
                local_addr,
                stats_folder,
                stats_interval,
                test_duration,
                no_flows,
                cc,
                bidirectional,
                test_volume,
                quit_bool.clone(),
            )
            .await?;
        }
        _ => unreachable!("unknown subcommand / missing args"),
    };

    Ok(())
}
