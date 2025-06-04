mod mysql;

use aidb_core::Aidb;
use futures::lock::Mutex;
use mysql::MySQLShim;

use std::sync::Arc;

use clap::Parser;
use eyre::{OptionExt, Result};
use opendal::{Operator, Scheme};
use opensrv_mysql::AsyncMysqlIntermediary;
use tokio::{net::TcpListener, select, sync::Notify};
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// TCP port to listen on
    #[arg(short, long, default_value_t = 3306)]
    port: u16,
    /// Host address to listen on
    #[arg(short, long, default_value = "[::]")]
    address: String,
    /// OpenDAL scheme
    #[arg(short, long, default_value = "fs")]
    scheme: String,
    /// OpenDAL configuration
    #[arg(short, long, default_values_t = ["root=./data/".to_owned()])]
    config: Vec<String>,
    #[command(flatten)]
    verbose: clap_verbosity_flag::Verbosity<clap_verbosity_flag::InfoLevel>,
}

fn init_storage(opendal_scheme: impl AsRef<str>, opendal_config: Vec<String>) -> Result<Operator> {
    let map: Option<Vec<(String, String)>> = opendal_config
        .into_iter()
        .map(|s| s.split_once('=').map(|(k, v)| (k.to_owned(), v.to_owned())))
        .collect();
    let map = map.ok_or_eyre("config should be kv pairs")?;
    Ok(Operator::via_iter(
        opendal_scheme.as_ref().parse::<Scheme>()?,
        map,
    )?)
}

fn init_core(args: &Args) -> Result<Aidb> {
    Ok(Aidb::from_op(init_storage(
        &args.scheme,
        args.config.clone(),
    )?))
}

fn get_shim(core: Arc<Mutex<Aidb>>) -> MySQLShim {
    MySQLShim { core }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let log_level = args.verbose.tracing_level().unwrap_or(tracing::Level::INFO);
    tracing_subscriber::fmt()
        .with_timer(tracing_subscriber::fmt::time::LocalTime::rfc_3339())
        .with_max_level(log_level)
        .init();
    info!("log level is {}", log_level.to_string());

    info!("initializing aidb");
    let core = Arc::new(Mutex::new(init_core(&args)?));

    let terminating = Arc::new(Notify::new());
    ctrlc::set_handler({
        let t = terminating.clone();
        move || t.notify_waiters()
    })?;
    let addr = format!("{}:{}", args.address, args.port);
    let listener = TcpListener::bind(&addr).await?;
    info!("listening on {addr}");

    loop {
        select! {
            result = listener.accept() => {
                let (stream, addr) = result?;
                info!("{addr} connected");
                let core = core.clone();
                tokio::spawn(async move {
                    let (r, w) = stream.into_split();
                    let shim = get_shim(core);
                    match AsyncMysqlIntermediary::run_on(shim, r, w).await {
                        Ok(()) => info!("{addr} disconnected"),
                        Err(e) => error!("{addr} disconnected with error: {e}"),
                    }
                });
            }
            _ = terminating.notified() => {
                break;
            }
        }
    }
    Ok(())
}
