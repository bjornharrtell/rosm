use strum::EnumString;

#[derive(EnumString)]
pub enum RelationType {
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

pub enum MemberType {
    Node = 1,
    Way = 2,
    Relation = 3,
}