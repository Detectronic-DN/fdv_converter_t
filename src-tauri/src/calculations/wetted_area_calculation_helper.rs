use std::f64::consts::PI;

pub struct WettedAreaCalculationHelper;

impl WettedAreaCalculationHelper {
    pub fn area(
        height: f64,
        radius1: f64,
        radius2: f64,
        radius3: f64,
        h1: f64,
        h2: f64,
        offset: f64,
        depth_of_water: f64,
    ) -> [f64; 2] {
        let mut wetted_area = 0.0;
        let mut perimeter = 0.0;
        let depth_of_water = if depth_of_water > height * 0.9999 {
            height * 0.9999
        } else {
            depth_of_water
        };

        let psi = ((h2 - radius1) / offset).atan();
        let area1 = 0.25 * radius3.powi(2) * (2.0 * psi - (2.0 * psi).sin());
        let inner_rect = (radius1.powi(2) - (radius1 - h1).powi(2)).sqrt();

        if depth_of_water <= h1 {
            let theta = 2.0 * ((radius1 - depth_of_water) / radius1).acos();
            wetted_area = 0.5 * (theta - theta.sin()) * radius1.powi(2);
            perimeter = 2.0 * radius1 * ((radius1 - depth_of_water) / radius1).acos();
        } else if h1 < depth_of_water && depth_of_water <= h2 {
            let z = h2 - depth_of_water;
            let phi = (z / radius3).asin();
            let area2 = 0.25 * radius3.powi(2) * (2.0 * phi - (2.0 * phi).sin());
            let x1 = (radius3.powi(2) - z.powi(2)).sqrt();
            let m = depth_of_water - h1;
            let p = x1 - offset - inner_rect;
            let area3 = m * inner_rect;
            let area4 = p * (h2 - depth_of_water);
            let area5 = area1 - area2 - area4;
            let theta = 2.0 * ((radius1 - h1) / radius1).acos();
            let area_lower_segment = 0.5 * (theta - theta.sin()) * radius1.powi(2);
            wetted_area = area_lower_segment + 2.0 * (area5 + area3);
            let alpha = psi - phi;
            let perimeter2 = radius3 * alpha * 2.0;
            let perimeter3 = 2.0 * radius1 * ((radius1 - h1) / radius1).acos();
            perimeter = perimeter3 + perimeter2;
        } else if depth_of_water > h2 {
            let i = depth_of_water - h1;
            let area6 = i * inner_rect;
            let area7 = area1;
            let area_middle_segment = 2.0 * (area7 + area6);
            let theta = 2.0 * ((radius1 - h1) / radius1).acos();
            let area_lower_segment2 = 0.5 * (theta - theta.sin()) * radius1.powi(2);
            let area8 = PI * radius2.powi(2) / 2.0;
            let z = depth_of_water - h2 + radius2;
            let z = radius2 * 2.0 - z;
            let gamma = 2.0 * ((radius2 - z) / radius2).acos();
            let area9 = PI * radius2.powi(2) - radius2.powi(2) * (gamma - gamma.sin()) / 2.0;
            let area_upper_segment = area9 - area8;
            let perimeter4 = PI * radius2 - radius2 * gamma;
            wetted_area = area_lower_segment2 + area_middle_segment + area_upper_segment;
            let alpha2 = psi;
            let perimeter5 = radius3 * alpha2 * 2.0;
            let perimeter6 = 2.0 * radius1 * ((radius1 - h1) / radius1).acos();
            perimeter = perimeter6 + perimeter5 + perimeter4;
        }

        [wetted_area, perimeter]
    }
}
