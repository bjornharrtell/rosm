use std::{collections::HashSet, error::Error};
use postgres::{Client, NoTls, binary_copy::BinaryCopyInWriter, types::Type};
use osmpbf::ElementReader;
use log::info;

use crate::bounds::{BoundsType, Bbox};
use crate::Import;
use crate::sql::{SCHEMA, CREATE_POINTS, CREATE_POLYGONS, CREATE_LINES};
use crate::poly::parse_wkt;

pub struct Importer {
    pub cs: String,
    pub bounds_type: BoundsType,
    pub nodes_index: HashSet<i64>,
    pub ways_index: HashSet<i64>,
}

pub struct ImportWriters<'a> {
    pub nodes: BinaryCopyInWriter<'a>,
    pub ways: BinaryCopyInWriter<'a>,
    pub rels: BinaryCopyInWriter<'a>,
    pub rels_members: BinaryCopyInWriter<'a>,
}

pub struct ImportClients {
    pub main: Client,
    pub nodes: Client,
    pub ways: Client,
    pub rels: Client,
    pub rels_members: Client,
}

impl ImportClients {
    fn new(cs: &str) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            main: Client::connect(cs, NoTls)?,
            nodes: Client::connect(cs, NoTls)?,
            ways: Client::connect(cs, NoTls)?,
            rels: Client::connect(cs, NoTls)?,
            rels_members: Client::connect(cs, NoTls)?,
        })
    }
}

impl<'a> ImportWriters<'a> {
    fn new(clients: &'a mut ImportClients) -> Result<Self, Box<dyn Error>> {
        let ways_sink = clients.ways.copy_in("copy osm.ways (id, refs, tags) from stdin binary")?;
        let nodes_sink = clients.nodes.copy_in("copy osm.nodes (id, lon, lat, tags) from stdin binary")?;
        let rels_sink = clients.rels.copy_in("copy osm.rels (id, type_id, tags) from stdin binary")?;
        let rels_members_sink = clients.rels_members.copy_in("copy osm.rels_members (rel_id, member_id, member_type_id, role, sequence_id) from stdin binary")?;
        Ok(Self {
            nodes: BinaryCopyInWriter::new(nodes_sink, &[Type::INT8, Type::FLOAT8, Type::FLOAT8, Type::JSONB]),
            ways: BinaryCopyInWriter::new(ways_sink, &[Type::INT8, Type::INT8_ARRAY, Type::JSONB]),
            rels: BinaryCopyInWriter::new(rels_sink, &[Type::INT8, Type::INT2, Type::JSONB]),
            rels_members: BinaryCopyInWriter::new(rels_members_sink, &[Type::INT8, Type::INT8, Type::INT2, Type::TEXT, Type::INT4])
        })
    }
}

impl Importer {
    pub fn new(args: &Import) -> Result<Self, Box<dyn Error>> {
        let cs = args.connectionstring.clone();
        let bounds_type = if args.polygon.is_some() {
            let p = parse_wkt(args.polygon.as_ref().unwrap().as_str());
            BoundsType::Polygon(p)
        } else if args.bbox.is_some() {
            let e: Vec<f64> = args.bbox.as_ref().unwrap().split(",").map(|e| e.parse::<f64>().unwrap()).collect();
            BoundsType::Bbox(Bbox { xmin: e[0], ymin: e[1], xmax: e[2], ymax: e[3] })
        } else {
            BoundsType::None
        };
        Ok(Self {
            cs,
            bounds_type,
            nodes_index: HashSet::new(),
            ways_index: HashSet::new()
        })
    }

    pub fn import(&mut self, args: &Import) -> Result<(), Box<dyn Error>> {
        info!("Creating schema");
        let mut client = Client::connect(self.cs.as_str(), NoTls)?;
        client.batch_execute(SCHEMA).unwrap();

        let mut clients = ImportClients::new(self.cs.as_str())?;
        let mut writers = ImportWriters::new(&mut clients)?;

        let path = &args.input;
        let reader = ElementReader::from_path(path)?;

        info!("Reading {}", path);
        match self.bounds_type.clone() {
            BoundsType::Bbox(b) =>
                reader.for_each(|e| self.write(e, &b, &mut writers))?,
            BoundsType::Polygon(p) =>
                reader.for_each(|e| self.write(e, &p, &mut writers))?,
            BoundsType::None =>
                reader.for_each(|e| self.write(e, &true, &mut writers))?
        }
        let nodes = writers.nodes.finish()?;
        let ways = writers.ways.finish()?;
        let rels = writers.rels.finish()?;
        let rels_members = writers.rels_members.finish()?;
        info!("Imported {} nodes", nodes);
        info!("Imported {} ways", ways);
        info!("Imported {} rels", rels);
        info!("Imported {} rels_members", rels_members);

        info!("Creating rels index");
        client.batch_execute("create index rels_type_idx on osm.rels (type_id)")?;

        info!("Analyzing nodes");
        client.batch_execute("analyze osm.nodes")?;
        info!("Analyzing ways");
        client.batch_execute("analyze osm.ways")?;
        info!("Analyzing rels");
        client.batch_execute("analyze osm.rels")?;

        // TODO: drop rels with unknown rel_refs

        info!("Creating nodes_points");
        let nodes_points = client.execute(CREATE_POINTS, &[])?;
        info!("Created {} nodes_points", nodes_points);
        info!("Creating ways_lines");
        let ways_lines = client.execute(CREATE_LINES, &[])?;
        info!("Created {} ways_lines", ways_lines);
        info!("Creating ways_polygons from closed lines");
        let ways_polygons = client.execute(CREATE_POLYGONS, &[])?;
        info!("Created {} ways_polygons", ways_polygons);

        info!("Vacuum ways_lines");
        client.batch_execute("vacuum full osm.ways_lines")?;

        info!("Creating rels_members index");
        client.batch_execute("create index rels_members_rel_id_member_id_member_type_idx on osm.rels_members (rel_id, member_id, member_type_id)")?;
        info!("Analyzing rels_members");
        client.batch_execute("analyze osm.rels_members")?;

        info!("Creating points spatial index");
        client.batch_execute("create index nodes_points_geom_idx on osm.nodes_points using gist(geom)")?;
        info!("Creating lines spatial index");
        client.batch_execute("create index ways_lines_geom_idx on osm.ways_lines using gist(geom)")?;
        info!("Creating polygons spatial index");
        client.batch_execute("create index ways_polygons_geom_idx on osm.ways_polygons using gist(geom)")?;

        info!("Analyzing nodes_points");
        client.batch_execute("analyze osm.nodes_points")?;
        info!("Analyzing ways_lines");
        client.batch_execute("analyze osm.ways_lines")?;
        info!("Analyzing ways_polygons");
        client.batch_execute("analyze osm.ways_polygons")?;

        //info!("Drop nodes, ways and rels");
        //client.batch_execute("drop table osm.nodes;drop table osm.ways;drop table osm.rels;")?;

        Ok(())
    }
}
