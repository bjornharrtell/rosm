pub const SCHEMA: &'static str = "set client_min_messages = warning;
drop view if exists osm.v_nodes_points;
drop view if exists osm.v_ways_lines;
drop view if exists osm.v_ways_polygons;
--drop view if exists osm.v_rels_multipolygons;
drop table if exists osm.nodes;
create table osm.nodes (
    id int8 primary key,
    lon float8 not null,
    lat float8 not null,
    tags jsonb
);
drop table if exists osm.ways;
create table osm.ways (
    id int8 primary key,
    refs _int8 not null,
    tags jsonb
);
drop table if exists osm.rels;
drop table if exists osm.rel_types;
create table osm.rel_types (
    id int2 generated always as identity primary key,
    name text not null
);
insert into osm.rel_types (name) select unnest(array[
    'unknown',
    'multipolygon',
    'route',
    'route_master',
    'restriction',
    'boundary',
    'public_transport',
    'destination_sign',
    'waterway',
    'enforcement',
    'connectivity'
]);
create table osm.rels (
    id int8 primary key,
    type_id int2 not null,
    tags jsonb,
    constraint fk_rel_type foreign key(type_id) references osm.rel_types(id)
);
drop table if exists osm.rels_members;
drop table if exists osm.rel_member_types;
create table osm.rel_member_types (
    id int2 generated always as identity primary key,
    name text not null
);
insert into osm.rel_member_types (name) select unnest(array[
    'node',
    'way',
    'relation'
]);
create table osm.rels_members (
    rel_id int8 not null,
    member_id int8 not null,
    member_type_id int2 not null,
    role text,
    sequence_id int4 not null,
    constraint fk_rel_member_type foreign key(member_type_id) references osm.rel_member_types(id)
);
drop table if exists osm.nodes_points;
create table osm.nodes_points (
    id int8 primary key,
    geom public.geometry(point,25832) not null
);
drop table if exists osm.ways_lines;
create table osm.ways_lines (
    id int8 primary key,
    geom public.geometry(linestring,25832) not null
);
drop table if exists osm.ways_polygons;
create table osm.ways_polygons (
    id int8 primary key,
    geom public.geometry(polygon,25832) not null
);
--drop table if exists osm.rels_multipolygons;
--create table osm.rels_multipolygons (
--    id int8 primary key,
--    geom public.geometry(multipolygon,25832) not null
--);
create or replace view osm.v_nodes_points as
    select p.id, p.geom, n.tags
    from osm.nodes_points p
    join osm.nodes n on (n.id = p.id);
create or replace view osm.v_ways_lines as
    select l.id, l.geom, w.tags
    from osm.ways_lines l
    join osm.ways w on (w.id = l.id);
create or replace view osm.v_ways_polygons as
    select p.id, p.geom, w.tags
    from osm.ways_polygons p
    join osm.ways w on (w.id = p.id);
--create or replace view osm.v_rels_multipolygons as
--    select mp.id, mp.geom, r.tags
--    from osm.rels_multipolygons mp
--    join osm.rels r on (r.id = mp.id);
";

pub const CREATE_POINTS: &str = "insert into osm.nodes_points
select id, st_makepoint(lon,lat)
from osm.nodes where tags is not null";

pub const CREATE_LINES: &str = "insert into osm.ways_lines
select w.id, st_makeline(st_makepoint(n.lon, n.lat) order by ordinality) geom
from osm.ways w, unnest(w.refs) with ordinality as node_id
join osm.nodes n on (n.id = node_id)
group by w.id;";

pub const CREATE_POLYGONS: &str = "with moved_rows as (
    delete
    from osm.ways_lines l
    using osm.ways w
    where
        w.id = l.id and
        st_npoints(geom) > 3 and
        st_isclosed(geom) and (
            not (
                coalesce(w.tags,'{}'::jsonb) ? 'highway' or
                not coalesce(w.tags,'{}'::jsonb) ? 'barrier'
            ) or
            w.tags->>'area' = 'yes'
        )
    returning l.*
)
insert into osm.ways_polygons
select id, st_makepolygon(geom) from moved_rows;";
