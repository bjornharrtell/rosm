use std::{collections::HashMap, error::Error, str::FromStr};

use osmpbf::{Element, DenseNode, DenseTagIter, TagIter, Way, Node, Relation};
use postgres::binary_copy::BinaryCopyInWriter;
use serde_json::Value;

use crate::{import::Importer, bounds::Bounds, osm::{MemberType, RelationType}};


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

impl Importer {
    pub fn write<T: Bounds>(&mut self, e: Element, b: &T,
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

    pub fn write_dense_node<T: Bounds>(&mut self, w: &mut BinaryCopyInWriter, dn: DenseNode, bounds: &T) -> Result<(), Box<dyn Error>> {
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

    pub fn write_way(&mut self, w: &mut BinaryCopyInWriter, way: Way) -> Result<(), Box<dyn Error>>  {
        let refs = way.refs().collect::<Vec<i64>>();
        if !refs.iter().any(|f| !self.node_index.contains(f)) {
            let id = way.id();
            self.way_index.insert(id);
            let tags = to_json_val(way.tags())?;
            w.write(&[&id, &refs, &tags])?;
        }
        Ok(())
    }

    pub fn write_node<T: Bounds>(&mut self, w: &mut BinaryCopyInWriter, n: Node, bounds: &T) -> Result<(), Box<dyn Error>>  {
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

    pub fn write_rel(&mut self, rels_writer: &mut BinaryCopyInWriter, rels_members_writer: &mut BinaryCopyInWriter, r: Relation) -> Result<(), Box<dyn Error>> {
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