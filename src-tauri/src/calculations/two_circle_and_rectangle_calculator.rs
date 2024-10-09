use super::calculator::{CalculationError, Calculator};
use std::f64::consts::PI;

fn calculate_segment_area(radius: f64, height: f64) -> f64 {
    let radius_squared = radius.powi(2);
    let t = radius - height;
    let chord_length = 2.0 * (radius_squared - t.powi(2)).sqrt();
    let c = chord_length / 2.0;
    let interior_angle = 2.0 * (c / t).atan();
    radius_squared * (interior_angle - interior_angle.sin()) / 2.0
}

pub struct TwoCircleAndRectangleCalculator {
    height: f64,
    width: f64,
}

impl TwoCircleAndRectangleCalculator {
    pub fn new(width: f64, height: f64) -> Result<Self, CalculationError> {
        if width.is_nan() || height.is_nan() || width <= 0.0 || height <= 0.0 {
            return Err(CalculationError::new("Invalid width or height."));
        }

        Ok(TwoCircleAndRectangleCalculator { height, width })
    }
}

impl Calculator for TwoCircleAndRectangleCalculator {
    fn perform_calculation(&self, depth: f64, velocity: f64) -> Result<f64, CalculationError> {
        if depth < 0.0 || velocity < 0.0 {
            return Err(CalculationError::new(
                "Depth and velocity must be non-negative.",
            ));
        }

        let r1 = self.width / 2.0;
        let d = depth;
        let v = velocity;
        let radius_squared = r1.powi(2);
        let circle_area = PI * radius_squared;

        let flow = if d < r1 {
            if d > 0.0 {
                calculate_segment_area(r1, d) * v * 1000.0
            } else {
                0.0
            }
        } else if d < self.height - r1 {
            let rectangle_area = (d - r1) * self.width;
            let bottom_half_circle_area = circle_area / 2.0;
            (bottom_half_circle_area + rectangle_area) * v * 1000.0
        } else if d < self.height {
            let d = d - self.width / 2.0 - (self.height - self.width);
            let top_half_circle_area = circle_area / 2.0 - calculate_segment_area(r1, r1 - d);
            let rectangle_area2 = (self.height - self.width) * self.width;
            let bottom_half_circle_area2 = circle_area / 2.0;
            (bottom_half_circle_area2 + rectangle_area2 + top_half_circle_area) * v * 1000.0
        } else {
            let top_half_circle_area = circle_area / 2.0;
            let rectangle_area2 = (self.height - self.width) * self.width;
            let bottom_half_circle_area2 = circle_area / 2.0;
            (bottom_half_circle_area2 + rectangle_area2 + top_half_circle_area) * v * 1000.0
        };

        Ok(flow)
    }
}
