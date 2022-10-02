use crate::poly::wn;


#[derive(Clone)]
pub struct Bbox {
    pub xmin: f64,
    pub ymin: f64,
    pub xmax: f64,
    pub ymax: f64,
}

#[derive(Clone)]
pub enum BoundsType {
    Bbox(Bbox),
    Polygon(Vec<f64>),
    None
}

pub trait Bounds {
    fn contains(&self, lon: f64, lat: f64) -> bool;
}

impl Bounds for Bbox {
    fn contains(&self, lon: f64, lat: f64) -> bool { 
        lon >= self.xmin && lon <= self.xmax &&
        lat >= self.ymin && lat <= self.ymax
    }
}

impl Bounds for Vec<f64> {
    fn contains(&self, lon: f64, lat: f64) -> bool { wn(self, lon, lat) != 0 }
}

impl Bounds for bool {
    fn contains(&self, _lon: f64, _lat: f64) -> bool { true }
}