# rosm

OpenStreetMap to PostgreSQL import tool written in Rust.

```
Usage: rosm <COMMAND>

Commands:
  import  Import a OSM pbf file into PostgreSQL schema
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help information
  -V, --version  Print version information
```

## How to get

Requires [Rust](https://www.rust-lang.org/) or [Docker](#Docker).

## Executing locally

### Example for localhost

> RUST_LOG=info cargo run --release -- import -i andorra-latest.osm.pbf -c "host=localhost user=postgres password=postgres dbname=osm"

## Executing via Docker

Read more about [Docker](https://www.docker.com/).

### How to run

> docker run -e URL="URL to osm.pbf file" -e CS="PostgreSQL connection string" rosm

NOTE: Doesn't work against a host local PostgreSQL due to Docker network constraints. This can be worked around in Linux as per below section.

### Running on Linux against PostgreSQL on localhost

> docker run --network="host" -e URL="https://download.geofabrik.de/europe/andorra-latest.osm.pbf" -e CS="host=localhost user=postgres password=postgres dbname=osm" bjornharrtell/rosm
