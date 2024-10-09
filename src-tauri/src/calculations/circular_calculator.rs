use super::calculator::{CalculationError, Calculator};
use std::f64::consts::PI;

pub struct CircularCalculator {
    pipe_radius: f64,
    radius_squared: f64,
    circle_area: f64,
}

impl CircularCalculator {
    pub fn new(pipe_radius: f64) -> Result<Self, CalculationError> {
        if pipe_radius.is_nan() {
            return Err(CalculationError::new("Pipe Radius Invalid."));
        }

        let radius_squared = pipe_radius.powi(2);
        let circle_area = PI * radius_squared;

        Ok(CircularCalculator {
            pipe_radius,
            radius_squared,
            circle_area,
        })
    }

    fn calculate_flow_value(&self, depth_value: f64, velocity_value: f64) -> f64 {
        if depth_value > self.pipe_radius {
            if depth_value < self.pipe_radius * 2.0 {
                let t = depth_value - self.pipe_radius;
                let chord_length = 2.0 * (self.radius_squared - t.powi(2)).sqrt();
                let c = chord_length / 2.0;
                let interior_angle = 2.0 * (c / t).atan();
                let segment_area =
                    self.radius_squared * (interior_angle - interior_angle.sin()) / 2.0;
                (self.circle_area - segment_area) * velocity_value * 1000.0
            } else {
                self.circle_area * velocity_value * 1000.0
            }
        } else if depth_value == self.pipe_radius {
            self.circle_area / 2.0 * velocity_value * 1000.0
        } else if depth_value > 0.0 {
            let t = self.pipe_radius - depth_value;
            let chord_length = 2.0 * (self.radius_squared - t.powi(2)).sqrt();
            let c = chord_length / 2.0;
            let interior_angle = 2.0 * (c / t).atan();
            let segment_area = self.radius_squared * (interior_angle - interior_angle.sin()) / 2.0;
            segment_area * velocity_value * 1000.0
        } else {
            0.0
        }
    }
}

impl Calculator for CircularCalculator {
    fn perform_calculation(&self, depth: f64, velocity: f64) -> Result<f64, CalculationError> {
        Ok(self.calculate_flow_value(depth, velocity))
    }
}
