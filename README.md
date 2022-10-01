# rosm

OpenStreetMap to PostgreSQL import tool written in Rust.

## How to run

Requires [Rust](https://www.rust-lang.org/) or [Docker](#Docker).

### localhost

> RUST_LOG=info cargo run --release -- import -i andorra-latest.osm.pbf -c "host=localhost user=postgres password=postgres dbname=osm"

## Docker

Read more about [Docker](https://www.docker.com/).

### Running

> docker run -e URL="URL to osm.pbf file" -e CS="PostgreSQL connection string" rosm

NOTE: Doesn't work against a host local PostgreSQL due to Docker network constraints. This can be worked around in Linux as per below section.

### Running on Linux against PostgreSQL on localhost

> docker run --network="host" -e URL="https://download.geofabrik.de/europe/andorra-latest.osm.pbf" -e CS="host=localhost user=postgres password=postgres dbname=osm" rosm
