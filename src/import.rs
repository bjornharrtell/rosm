use std::{collections::{HashSet, HashMap}, error::Error};
use postgres::{Client, NoTls, binary_copy::BinaryCopyInWriter, types::Type};
use osmpbf::{ElementReader, Element, DenseNode, Way, Node, Relation, TagIter, DenseTagIter};
use log::info;
use serde_json::Value;
use crate::{poly::{parse_wkt, wn}, sql::{SCHEMA, CREATE_LINES, CREATE_POLYGONS, CREATE_POINTS}, Import};
use strum_macros::EnumString;
use std::str::FromStr;

fn to_json_val_dti(it: DenseTagIter) -> Result<Option<Value>, Box<dyn Error>> {
    if it.len() == 0 {
        Ok(None)
    } else {
        let tags: HashMap<&str, &str> = it.collect();
        Ok(Some(serde_json::to_value(tags)?))
    }
}

fn write_dense_node<T: Bounds>(w: &mut BinaryCopyInWriter, dn: DenseNode, index: &mut HashSet<i64>, bounds: &T) -> Result<(), Box<dyn Error>> {
    let lon = dn.lon();
    let lat = dn.lat();
    if bounds.contains(lon, lat) {
        let id = dn.id();
        index.insert(id);
        let tags = to_json_val_dti(dn.tags())?;
        w.write(&[&id, &lon, &lat, &tags])?;
    }
    Ok(())
}

fn to_json_val(it: TagIter) -> Result<Option<Value>, Box<dyn Error>> {
    if it.len() == 0 {
        Ok(None)
    } else {
        let tags: HashMap<&str, &str> = it.collect();
        Ok(Some(serde_json::to_value(tags)?))
    }
}

fn write_way(w: &mut BinaryCopyInWriter, way: Way, node_index: &HashSet<i64>, way_index: &mut HashSet<i64>) -> Result<(), Box<dyn Error>>  {
    let refs = way.refs().collect::<Vec<i64>>();
    if !refs.iter().any(|f| !node_index.contains(f)) {
        let id = way.id();
        way_index.insert(id);
        let tags = to_json_val(way.tags())?;
        w.write(&[&id, &refs, &tags])?;
    }
    Ok(())
}

fn write_node<T: Bounds>(w: &mut BinaryCopyInWriter, n: Node, index: &mut HashSet<i64>, bounds: &T) -> Result<(), Box<dyn Error>>  {
    let lon = n.lon();
    let lat = n.lat();
    if bounds.contains(lon, lat) {
        let id = n.id();
        index.insert(id);
        let tags = to_json_val(n.tags())?;
        w.write(&[&n.id(), &lon, &lat, &tags])?;
    }
    Ok(())
}

fn write_rel(rels_writer: &mut BinaryCopyInWriter, rels_members_writer: &mut BinaryCopyInWriter, r: Relation, node_index: &HashSet<i64>, way_index: &HashSet<i64>) -> Result<(), Box<dyn Error>> {
    use convert_case::{Case, Casing};
    let id = r.id();
    let mut sequence_id = 0;
    for m in r.members() {
        let member_id = m.member_id;
        let rel_type = match m.member_type {
            osmpbf::RelMemberType::Node => if !node_index.contains(&member_id) { return Ok(()) } else { MemberType::Node as i16 },
            osmpbf::RelMemberType::Way => if !way_index.contains(&member_id) { return Ok(()) } else { MemberType::Way as i16 },
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

#[derive(EnumString)]
enum RelationType {
    Unknown = 1,
    Multipolygon = 2,
    Route = 3,
    RouteMaster = 4,
    Restriction = 5,
    Boundary = 6,
    PublicTransport = 7,
    DestinationSign = 8,
    Waterway = 9,
    Enforcement = 10,
    Connectivity = 11
}

enum MemberType {
    Node = 1,
    Way = 2,
    Relation = 3,
}

pub struct Bbox {
    xmin: f64,
    ymin: f64,
    xmax: f64,
    ymax: f64,
}

fn bbox_contains(bbox: &Bbox, lon: f64, lat: f64) -> bool {
    lon >= bbox.xmin && lon <= bbox.xmax &&
    lat >= bbox.ymin && lat <= bbox.ymax
}

trait Bounds {
    fn contains(&self, lon: f64, lat: f64) -> bool;
}

impl Bounds for Bbox {
    fn contains(&self, lon: f64, lat: f64) -> bool { bbox_contains(self, lon, lat) }
}

impl Bounds for Vec<f64> {
    fn contains(&self, lon: f64, lat: f64) -> bool { wn(self, lon, lat) != 0 }
}

impl Bounds for bool {
    fn contains(&self, _lon: f64, _lat: f64) -> bool { true }
}

pub fn import(args: &Import) -> Result<(), Box<dyn Error>> {
    //let denmark = "POLYGON ((7.87 54.69, 7.78 57.25, 9.63 58.08, 10.71 58.11, 12.05 56.69, 13.15 56.42, 14.2 55.47, 15.5 55.33, 15.28 54.64, 12.98 54.94, 12.29 54.35, 12.46 53.64, 11.41 53.42, 10.07 53.18, 8.78 53.52, 7.87 54.69))";
    //let p = parse_wkt(denmark);

    let cs = &args.connectionstring.as_str();

    let mut node_index = HashSet::new();
    let mut ways_index = HashSet::new();

    let mut client = Client::connect(cs, NoTls)?;

    info!("Creating schema");
    client.batch_execute(SCHEMA).unwrap();

    let mut nodes_client = Client::connect(cs, NoTls)?;
    let mut ways_client = Client::connect(cs, NoTls)?;
    let mut rels_client = Client::connect(cs, NoTls)?;
    let mut rels_members_client = Client::connect(cs, NoTls)?;
    let ways_sink = ways_client.copy_in("copy osm.ways (id, refs, tags) from stdin binary")?;
    let nodes_sink = nodes_client.copy_in("copy osm.nodes (id, lon, lat, tags) from stdin binary")?;
    let rels_sink = rels_client.copy_in("copy osm.rels (id, type_id, tags) from stdin binary")?;
    let rels_members_sink = rels_members_client.copy_in("copy osm.rels_members (rel_id, member_id, member_type_id, role, sequence_id) from stdin binary")?;
    let mut ways_writer = BinaryCopyInWriter::new(ways_sink, &[Type::INT8, Type::INT8_ARRAY, Type::JSONB]);
    let mut nodes_writer = BinaryCopyInWriter::new(nodes_sink, &[Type::INT8, Type::FLOAT8, Type::FLOAT8, Type::JSONB]);
    let mut rels_writer = BinaryCopyInWriter::new(rels_sink, &[Type::INT8, Type::INT2, Type::JSONB]);
    let mut rels_members_writer = BinaryCopyInWriter::new(rels_members_sink, &[Type::INT8, Type::INT8, Type::INT2, Type::TEXT, Type::INT4]);

    let path = &args.input;
    let reader = ElementReader::from_path(path)?;

    info!("Reading {}", path);
    if args.polygon.is_some() {
        let p = parse_wkt(args.polygon.as_ref().unwrap().as_str());
        reader.for_each(|element| {
            match element {
                Element::DenseNode(dn) => write_dense_node(&mut nodes_writer, dn, &mut node_index, &p).unwrap(),
                Element::Node(n) => write_node(&mut nodes_writer, n, &mut node_index, &p).unwrap(),
                Element::Way(w) => write_way(&mut ways_writer, w, &node_index, &mut ways_index).unwrap(),
                Element::Relation(r) => write_rel(&mut rels_writer, &mut rels_members_writer, r, &node_index, &ways_index).unwrap(),
            }
        })?;
    } else if args.bbox.is_some() {
        let e: Vec<f64> = args.bbox.as_ref().unwrap().split(",").map(|e| e.parse::<f64>().unwrap()).collect();
        let bbox = Bbox { xmin: e[0], ymin: e[1], xmax: e[2], ymax: e[3] };
        reader.for_each(|element| {
            match element {
                Element::DenseNode(dn) => write_dense_node(&mut nodes_writer, dn, &mut node_index, &bbox).unwrap(),
                Element::Node(n) => write_node(&mut nodes_writer, n, &mut node_index, &bbox).unwrap(),
                Element::Way(w) => write_way(&mut ways_writer, w, &node_index, &mut ways_index).unwrap(),
                Element::Relation(r) => write_rel(&mut rels_writer, &mut rels_members_writer, r, &node_index, &ways_index).unwrap(),
            }
        })?;
    } else {
        reader.for_each(|element| {
            match element {
                Element::DenseNode(dn) => write_dense_node(&mut nodes_writer, dn, &mut node_index, &true).unwrap(),
                Element::Node(n) => write_node(&mut nodes_writer, n, &mut node_index, &true).unwrap(),
                Element::Way(w) => write_way(&mut ways_writer, w, &node_index, &mut ways_index).unwrap(),
                Element::Relation(r) => write_rel(&mut rels_writer, &mut rels_members_writer, r, &node_index, &ways_index).unwrap(),
            }
        })?;
    };
    let nodes = nodes_writer.finish()?;
    let ways = ways_writer.finish()?;
    let rels = rels_writer.finish()?;
    let rels_members = rels_members_writer.finish()?;
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
