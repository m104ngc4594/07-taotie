use super::{ReplCommand, ReplResult};
use crate::ReplContext;
use clap::{ArgMatches, Parser};

#[derive(Debug, Clone)]
pub enum DatasetConn {
    Postgres(String),
    Csv(String),
    Parquet(String),
    Json(String),
}

#[derive(Debug, Parser)]
pub struct ConnectOpts {
    #[arg(value_parser = verify_conn_str, help = "Connection string to the dataset, could be postgres of local file (supported extensions: .csv, .parquet, .json)")]
    pub conn: DatasetConn,

    #[arg(short, long, help = "If database, the name of the table")]
    pub table: Option<String>,

    #[arg(short, long, help = "The name of the dataset")]
    pub name: String,
}

pub fn connect(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let conn = args
        .get_one::<DatasetConn>("conn")
        .expect("expect conn_str")
        .to_owned();
    let table = args.get_one::<String>("table").map(|s| s.to_string());
    let name = args
        .get_one::<String>("name")
        .expect("expect name")
        .to_string();

    let cmd = ConnectOpts::new(conn, table, name).into();
    ctx.send(cmd);

    Ok(None)
}

impl From<ConnectOpts> for ReplCommand {
    fn from(opts: ConnectOpts) -> Self {
        Self::Connect(opts)
    }
}

impl ConnectOpts {
    pub fn new(conn: DatasetConn, table: Option<String>, name: String) -> Self {
        Self { conn, table, name }
    }
}

fn verify_conn_str(conn_str: &str) -> Result<DatasetConn, String> {
    let conn_str = conn_str.to_string();
    if conn_str.starts_with("postgres://") {
        return Ok(DatasetConn::Postgres(conn_str));
    }
    if conn_str.ends_with(".csv") {
        return Ok(DatasetConn::Csv(conn_str));
    }
    if conn_str.ends_with(".parquet") {
        return Ok(DatasetConn::Parquet(conn_str));
    }
    if conn_str.ends_with(".json") {
        return Ok(DatasetConn::Json(conn_str));
    }
    Err(format!("Invalid connection string: {}", conn_str))
}
