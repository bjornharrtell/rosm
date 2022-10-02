use std::{collections::HashMap, error::Error, str::FromStr};

use osmpbf::{Element, DenseNode, DenseTagIter, TagIter, Way, Node, Relation};
use postgres::binary_copy::BinaryCopyInWriter;
use serde_json::Value;

use crate::{import::{Importer, ImportIndexes, ImportWriters}, bounds::Bounds, osm::{MemberType, RelationType}};


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
    pub fn write<T: Bounds>(&self, e: Element, b: &T,
        indexes: &mut ImportIndexes,
        writers: &mut ImportWriters,
    ) {
        match e {
            Element::DenseNode(dn) => self.write_dense_node(indexes, &mut writers.nodes, dn, b).unwrap(),
            Element::Node(n) => self.write_node(indexes, &mut writers.nodes, n, b).unwrap(),
            Element::Way(w) => self.write_way(indexes, &mut writers.ways, w).unwrap(),
            Element::Relation(r) => self.write_rel(indexes, &mut writers.rels, &mut writers.rels_members, r).unwrap(),
        }
    }

    pub fn write_dense_node<T: Bounds>(&self, indexes: &mut ImportIndexes, w: &mut BinaryCopyInWriter, dn: DenseNode, bounds: &T) -> Result<(), Box<dyn Error>> {
        let lon = dn.lon();
        let lat = dn.lat();
        if bounds.contains(lon, lat) {
            let id = dn.id();
            indexes.nodes.insert(id);
            let tags = to_json_val_dti(dn.tags())?;
            w.write(&[&id, &lon, &lat, &tags])?;
        }
        Ok(())
    }

    pub fn write_way(&self, indexes: &mut ImportIndexes, w: &mut BinaryCopyInWriter, way: Way) -> Result<(), Box<dyn Error>>  {
        let refs = way.refs().collect::<Vec<i64>>();
        if !refs.iter().any(|f| !indexes.nodes.contains(f)) {
            let id = way.id();
            indexes.ways.insert(id);
            let tags = to_json_val(way.tags())?;
            w.write(&[&id, &refs, &tags])?;
        }
        Ok(())
    }

    pub fn write_node<T: Bounds>(&self, indexes: &mut ImportIndexes, w: &mut BinaryCopyInWriter, n: Node, bounds: &T) -> Result<(), Box<dyn Error>>  {
        let lon = n.lon();
        let lat = n.lat();
        if bounds.contains(lon, lat) {
            let id = n.id();
            indexes.nodes.insert(id);
            let tags = to_json_val(n.tags())?;
            w.write(&[&n.id(), &lon, &lat, &tags])?;
        }
        Ok(())
    }

    pub fn write_rel(&self, indexes: &mut ImportIndexes, rels_writer: &mut BinaryCopyInWriter, rels_members_writer: &mut BinaryCopyInWriter, r: Relation) -> Result<(), Box<dyn Error>> {
        use convert_case::{Case, Casing};
        let id = r.id();
        let mut sequence_id = 0;
        for m in r.members() {
            let member_id = m.member_id;
            let rel_type = match m.member_type {
                osmpbf::RelMemberType::Node => if !indexes.nodes.contains(&member_id) { return Ok(()) } else { MemberType::Node as i16 },
                osmpbf::RelMemberType::Way => if !indexes.ways.contains(&member_id) { return Ok(()) } else { MemberType::Way as i16 },
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