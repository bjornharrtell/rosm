use std::{collections::{HashSet, HashMap}, error::Error};
use postgres::{Client, NoTls, binary_copy::BinaryCopyInWriter, types::Type};
use osmpbf::{ElementReader, Element, DenseNode, Way, Node, Relation, TagIter, DenseTagIter};
use log::info;
use serde_json::Value;
use crate::bounds::{BoundsType, Bbox, Bounds};
use crate::osm::{MemberType, RelationType};
use crate::Import;
use crate::sql::{SCHEMA, CREATE_POINTS, CREATE_POLYGONS, CREATE_LINES};
use crate::poly::parse_wkt;
use std::str::FromStr;

fn to_json_val_dti(it: DenseTagIter) -> Result<Option<Value>, Box<dyn Error>> {
    if it.len() == 0 {
        Ok(None)
    } else {
        let tags: HashMap<&str, &str> = it.collect();
        Ok(Some(serde_json::to_value(tags)?))
    }
}

fn to_json_val(it: TagIter) -> Result<Option<Value>, Box<dyn Error>> {
    if it.len() == 0 {
        Ok(None)
    } else {
        let tags: HashMap<&str, &str> = it.collect();
        Ok(Some(serde_json::to_value(tags)?))
    }
}

pub struct Importer {
    client: Client,
    node_index: HashSet<i64>,
    way_index: HashSet<i64>
}

impl Importer {
    pub fn new(args: &Import) -> Result<Self, Box<dyn Error>> {
        let cs = &args.connectionstring.as_str();
        let client = Client::connect(cs, NoTls)?;
        Ok(Self {
            client,
            node_index: HashSet::new(),
            way_index: HashSet::new()
        })
    }

    pub fn import(&mut self, args: &Import) -> Result<(), Box<dyn Error>> {
        let cs = &args.connectionstring.as_str();

        let bounds_type = if args.polygon.is_some() {
            let p = parse_wkt(args.polygon.as_ref().unwrap().as_str());
            BoundsType::Polygon(p)
        } else if args.bbox.is_some() {
            let e: Vec<f64> = args.bbox.as_ref().unwrap().split(",").map(|e| e.parse::<f64>().unwrap()).collect();
            BoundsType::Bbox(Bbox { xmin: e[0], ymin: e[1], xmax: e[2], ymax: e[3] })
        } else {
            BoundsType::None
        };

        info!("Creating schema");
        self.client.batch_execute(SCHEMA).unwrap();

        let mut nodes_client = Client::connect(cs, NoTls)?;
        let mut ways_client = Client::connect(cs, NoTls)?;
        let mut rels_client = Client::connect(cs, NoTls)?;
        let mut rels_members_client = Client::connect(cs, NoTls)?;
        let ways_sink = ways_client.copy_in("copy osm.ways (id, refs, tags) from stdin binary")?;
        let nodes_sink = nodes_client.copy_in("copy osm.nodes (id, lon, lat, tags) from stdin binary")?;
        let rels_sink = rels_client.copy_in("copy osm.rels (id, type_id, tags) from stdin binary")?;
        let rels_members_sink = rels_members_client.copy_in("copy osm.rels_members (rel_id, member_id, member_type_id, role, sequence_id) from stdin binary")?;

        let mut nodes_writer = BinaryCopyInWriter::new(nodes_sink, &[Type::INT8, Type::FLOAT8, Type::FLOAT8, Type::JSONB]);
        let mut ways_writer = BinaryCopyInWriter::new(ways_sink, &[Type::INT8, Type::INT8_ARRAY, Type::JSONB]);
        let mut rels_writer = BinaryCopyInWriter::new(rels_sink, &[Type::INT8, Type::INT2, Type::JSONB]);
        let mut rels_members_writer = BinaryCopyInWriter::new(rels_members_sink, &[Type::INT8, Type::INT8, Type::INT2, Type::TEXT, Type::INT4]);

        let path = &args.input;
        let reader = ElementReader::from_path(path)?;

        info!("Reading {}", path);
        match bounds_type {
            BoundsType::Bbox(b) => 
                reader.for_each(|e| self.write(e, &b, &mut nodes_writer, &mut ways_writer, &mut rels_writer, &mut rels_members_writer))?,
            BoundsType::Polygon(p) =>
                reader.for_each(|e| self.write(e, &p, &mut nodes_writer, &mut ways_writer, &mut rels_writer, &mut rels_members_writer))?,
            BoundsType::None =>
                reader.for_each(|e| self.write(e, &true, &mut nodes_writer, &mut ways_writer, &mut rels_writer, &mut rels_members_writer))?
        }
        let nodes = nodes_writer.finish()?;
        let ways = ways_writer.finish()?;
        let rels = rels_writer.finish()?;
        let rels_members = rels_members_writer.finish()?;
        info!("Imported {} nodes", nodes);
        info!("Imported {} ways", ways);
        info!("Imported {} rels", rels);
        info!("Imported {} rels_members", rels_members);

        info!("Creating rels index");
        self.client.batch_execute("create index rels_type_idx on osm.rels (type_id)")?;

        info!("Analyzing nodes");
        self.client.batch_execute("analyze osm.nodes")?;
        info!("Analyzing ways");
        self.client.batch_execute("analyze osm.ways")?;
        info!("Analyzing rels");
        self.client.batch_execute("analyze osm.rels")?;

        // TODO: drop rels with unknown rel_refs

        info!("Creating nodes_points");
        let nodes_points = self.client.execute(CREATE_POINTS, &[])?;
        info!("Created {} nodes_points", nodes_points);
        info!("Creating ways_lines");
        let ways_lines = self.client.execute(CREATE_LINES, &[])?;
        info!("Created {} ways_lines", ways_lines);
        info!("Creating ways_polygons from closed lines");
        let ways_polygons = self.client.execute(CREATE_POLYGONS, &[])?;
        info!("Created {} ways_polygons", ways_polygons);

        info!("Vacuum ways_lines");
        self.client.batch_execute("vacuum full osm.ways_lines")?;

        info!("Creating rels_members index");
        self.client.batch_execute("create index rels_members_rel_id_member_id_member_type_idx on osm.rels_members (rel_id, member_id, member_type_id)")?;
        info!("Analyzing rels_members");
        self.client.batch_execute("analyze osm.rels_members")?;

        info!("Creating points spatial index");
        self.client.batch_execute("create index nodes_points_geom_idx on osm.nodes_points using gist(geom)")?;
        info!("Creating lines spatial index");
        self.client.batch_execute("create index ways_lines_geom_idx on osm.ways_lines using gist(geom)")?;
        info!("Creating polygons spatial index");
        self.client.batch_execute("create index ways_polygons_geom_idx on osm.ways_polygons using gist(geom)")?;

        info!("Analyzing nodes_points");
        self.client.batch_execute("analyze osm.nodes_points")?;
        info!("Analyzing ways_lines");
        self.client.batch_execute("analyze osm.ways_lines")?;
        info!("Analyzing ways_polygons");
        self.client.batch_execute("analyze osm.ways_polygons")?;

        //info!("Drop nodes, ways and rels");
        //client.batch_execute("drop table osm.nodes;drop table osm.ways;drop table osm.rels;")?;

        Ok(())
    }

    fn write<T: Bounds>(&mut self, e: Element, b: &T,
        nodes_writer: &mut BinaryCopyInWriter,
        ways_writer: &mut BinaryCopyInWriter,
        rels_writer: &mut BinaryCopyInWriter,
        rels_members_writer: &mut BinaryCopyInWriter
    ) {
        match e {
            Element::DenseNode(dn) => self.write_dense_node(nodes_writer, dn, b).unwrap(),
            Element::Node(n) => self.write_node(nodes_writer, n, b).unwrap(),
            Element::Way(w) => self.write_way(ways_writer, w).unwrap(),
            Element::Relation(r) => self.write_rel(rels_writer, rels_members_writer, r).unwrap(),
        }
    }

    fn write_dense_node<T: Bounds>(&mut self, w: &mut BinaryCopyInWriter, dn: DenseNode, bounds: &T) -> Result<(), Box<dyn Error>> {
        let lon = dn.lon();
        let lat = dn.lat();
        if bounds.contains(lon, lat) {
            let id = dn.id();
            self.node_index.insert(id);
            let tags = to_json_val_dti(dn.tags())?;
            w.write(&[&id, &lon, &lat, &tags])?;
        }
        Ok(())
    }

    fn write_way(&mut self, w: &mut BinaryCopyInWriter, way: Way) -> Result<(), Box<dyn Error>>  {
        let refs = way.refs().collect::<Vec<i64>>();
        if !refs.iter().any(|f| !self.node_index.contains(f)) {
            let id = way.id();
            self.way_index.insert(id);
            let tags = to_json_val(way.tags())?;
            w.write(&[&id, &refs, &tags])?;
        }
        Ok(())
    }

    fn write_node<T: Bounds>(&mut self, w: &mut BinaryCopyInWriter, n: Node, bounds: &T) -> Result<(), Box<dyn Error>>  {
        let lon = n.lon();
        let lat = n.lat();
        if bounds.contains(lon, lat) {
            let id = n.id();
            self.node_index.insert(id);
            let tags = to_json_val(n.tags())?;
            w.write(&[&n.id(), &lon, &lat, &tags])?;
        }
        Ok(())
    }

    fn write_rel(&mut self, rels_writer: &mut BinaryCopyInWriter, rels_members_writer: &mut BinaryCopyInWriter, r: Relation) -> Result<(), Box<dyn Error>> {
        use convert_case::{Case, Casing};
        let id = r.id();
        let mut sequence_id = 0;
        for m in r.members() {
            let member_id = m.member_id;
            let rel_type = match m.member_type {
                osmpbf::RelMemberType::Node => if !self.node_index.contains(&member_id) { return Ok(()) } else { MemberType::Node as i16 },
                osmpbf::RelMemberType::Way => if !self.way_index.contains(&member_id) { return Ok(()) } else { MemberType::Way as i16 },
                osmpbf::RelMemberType::Relation => MemberType::Relation as i16
            };
            // TODO: defer write to avoid writing anything when contains checks above detects unresolvable members
            rels_members_writer.write(&[&id, &m.member_id, &rel_type, &m.role()?, &sequence_id])?;
            sequence_id += 1;
        }
        let mut tags = r.tags();
        let type_tag = tags.find(|e| e.0 == "type");
        let tags_jsonb = to_json_val(tags)?;
        let rel_type = match type_tag {
            Some(t) => RelationType::from_str(t.1.to_case(Case::Pascal).as_str()).unwrap_or_else(|_| RelationType::Unknown) as i16,
            None => RelationType::Unknown as i16
        };
        rels_writer.write(&[&id, &rel_type, &tags_jsonb])?;
        Ok(())
    }
}