#[inline]
// Winding number algorithm
// https://en.wikipedia.org/wiki/Point_in_polygon#Winding_number_algorithm
// https://web.archive.org/web/20130126163405/http://geomalgorithms.com/a03-_inclusion.html
pub fn wn(p: &Vec<f64>, x: f64, y: f64) -> i32 {
    #[inline]
    fn is_left(x0: f64, y0: f64, x1: f64, y1: f64, x2: f64, y2: f64) -> f64 {
        ((x1 - x0) * (y2 - y0)) - ((x2 - x0) * (y1 - y0))
    }
    let mut wn = 0;
    for i in (0..p.len()-2).step_by(2) {
        let ex1 = p[i];
        let ey1 = p[i + 1];
        let ex2 = p[i + 2];
        let ey2 = p[i + 3];
        if ey1 <= y {
            if ey2 > y {
                if is_left(ex1, ey1, ex2, ey2, x, y) > 0.0 {
                    wn += 1;
                }
            }
        } else {
            if ey2 <= y {
                if is_left(ex1, ey1, ex2, ey2, x, y) < 0.0 {
                    wn -= 1;
                }
            }
        }
    }
    wn
}

pub fn parse_wkt(wkt: &str) -> Vec<f64> {
    use wkt::{TryFromWkt};
    use geo_types::Polygon;
    let p: Polygon<f64> = Polygon::try_from_wkt_str(wkt).unwrap();
    let cs: Vec<f64> = p.exterior().coords().flat_map(|c| [c.x, c.y]).collect();
    cs
}

#[test]
fn wn_test() {
    let p = vec![0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0];
    let r = wn(&p, 0.5, 0.5);
    assert_eq!(r, -1);
    let r = wn(&p, 1.5, 1.5);
    assert_eq!(r, 0);
    let r = wn(&p, -1.5, -1.5);
    assert_eq!(r, 0);
    let r = wn(&p, 0.9, 0.1);
    assert_eq!(r, -1);
}

#[test]
fn wn_test2() {
    let denmark = "POLYGON ((7.87 54.69, 7.78 57.25, 9.63 58.08, 10.71 58.11, 12.05 56.69, 13.15 56.42, 14.2 55.47, 15.5 55.33, 15.28 54.64, 12.98 54.94, 12.29 54.35, 12.46 53.64, 11.41 53.42, 10.07 53.18, 8.78 53.52, 7.87 54.69))";
    let p = parse_wkt(denmark);
    assert_eq!(p, vec![
        7.87, 54.69, 7.78, 57.25,
        9.63, 58.08, 10.71, 58.11,
        12.05, 56.69, 13.15, 56.42,
        14.2, 55.47, 15.5, 55.33,
        15.28, 54.64, 12.98, 54.94,
        12.29, 54.35, 12.46, 53.64,
        11.41, 53.42, 10.07, 53.18,
        8.78, 53.52, 7.87, 54.69]);
    let r = wn(&p, 10.0, 56.0);
    assert_eq!(r, -1);
    let r = wn(&p, 56.0, 10.0);
    assert_eq!(r, 0);
}