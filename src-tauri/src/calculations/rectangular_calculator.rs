use super::calculator::{CalculationError, Calculator};

pub struct RectangularCalculator {
    channel_width: f64,
}

impl RectangularCalculator {
    pub fn new(width: f64) -> Result<Self, CalculationError> {
        if width.is_nan() {
            return Err(CalculationError::new("Channel Width Invalid."));
        }

        Ok(RectangularCalculator {
            channel_width: width,
        })
    }
}

impl Calculator for RectangularCalculator {
    fn perform_calculation(&self, depth: f64, velocity: f64) -> Result<f64, CalculationError> {
        let flow = depth * velocity * self.channel_width * 1000.0;
        Ok(flow.max(0.0))
    }
}
