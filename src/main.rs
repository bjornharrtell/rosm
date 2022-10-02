use std::error::Error;
use clap::{Parser, Subcommand, Args};
use simple_logger::SimpleLogger;
use log::info;
use crate::import::Importer;

mod bounds;
mod import_write;
mod import;
mod osm;
mod poly;
mod sql;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Import a OSM pbf file into PostgreSQL schema
    Import(Import),
    // Update an existing PostgreSQL schema
    //Update(Update)
}

#[derive(Args)]
pub struct Import {
    /// Input path
    #[clap(short, long)]
    input: String,

    /// Target PostgreSQL connection string
    #[clap(short, long)]
    connectionstring: String,

    /// Filter by bbox (lat/lon comma separated)
    #[clap(short, long)]
    bbox: Option<String>,

    /// Filter by polygon (WKT, lat/lon)
    #[clap(short, long)]
    polygon: Option<String>,

    /* TODO
    /// Make import updatable
    #[clap(long, default_value_t = true)]
    updatable: bool,
    */
}

/*#[derive(Args)]
struct Update {
    /// Target PostgreSQL connection string
    #[clap(short, long)]
    connectionstring: Option<String>,
}*/

fn main() -> Result<(), Box<dyn Error>> {
    SimpleLogger::new().env().init()?;
    let cli = Cli::parse();
    info!("Initializing");
    match &cli.command {
        Commands::Import(args) => {
            let mut importer = Importer::new(args)?;
            importer.import(args)?
        },
        //Commands::Update(_) => todo!(),
    }
    info!("Done!");
    Ok(())
}
