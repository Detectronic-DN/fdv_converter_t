use super::calculator::{CalculationError, Calculator};
use super::wetted_area_calculation_helper::WettedAreaCalculationHelper;

pub struct Egg2ACalculator {
    height: f64,
    radius1: f64,
    radius2: f64,
    radius3: f64,
    offset: f64,
    h2: f64,
    h1: f64,
}

impl Egg2ACalculator {
    pub fn new(height: f64, width: f64, radius3: f64) -> Result<Self, CalculationError> {
        if height.is_nan()
            || width.is_nan()
            || radius3.is_nan()
            || height <= 0.0
            || width <= 0.0
            || radius3 <= 0.0
        {
            return Err(CalculationError::new(
                "Invalid Parameters Supplied to Constructor",
            ));
        }

        let radius1 = (height - width) / 4.0;
        let radius2 = width / 2.0;
        let offset = radius3 - radius2;
        let h2 = height - radius2;
        let h1 = h2 - radius3 * ((h2 - radius1) / offset).atan().sin();

        Ok(Egg2ACalculator {
            height,
            radius1,
            radius2,
            radius3,
            offset,
            h2,
            h1,
        })
    }
}

impl Calculator for Egg2ACalculator {
    fn perform_calculation(&self, depth: f64, velocity: f64) -> Result<f64, CalculationError> {
        let [area, _] = WettedAreaCalculationHelper::area(
            self.height,
            self.radius1,
            self.radius2,
            self.radius3,
            self.h1,
            self.h2,
            self.offset,
            depth,
        );
        let result = area * velocity * 1000.0;
        Ok(result.max(0.0))
    }
}
