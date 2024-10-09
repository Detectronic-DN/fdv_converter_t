use super::calculator::{CalculationError, Calculator};
use super::egg_calculator::EggCalculator;

pub struct Egg2Calculator {
    height: f64,
    radius1: f64,
    radius2: f64,
    radius3: f64,
    offset: f64,
    height2: f64,
    height1: f64,
}

impl Egg2Calculator {
    pub fn new(height: f64) -> Result<Self, CalculationError> {
        if height.is_nan() {
            return Err(CalculationError::new(
                "Invalid Parameters Supplied to Constructor",
            ));
        }

        let radius1 = height / 12.0;
        let radius2 = height / 3.0;
        let radius3 = 8.0 * height / 9.0;
        let offset = 5.0 * height / 9.0;
        let height2 = height - radius2;
        let height1 = height2 - radius3 * ((height2 - radius1) / offset).atan().sin();

        Ok(Egg2Calculator {
            height,
            radius1,
            radius2,
            radius3,
            offset,
            height2,
            height1,
        })
    }
}

impl EggCalculator for Egg2Calculator {
    fn height(&self) -> f64 {
        self.height
    }
    fn radius1(&self) -> f64 {
        self.radius1
    }
    fn radius2(&self) -> f64 {
        self.radius2
    }
    fn radius3(&self) -> f64 {
        self.radius3
    }
    fn offset(&self) -> f64 {
        self.offset
    }
    fn height1(&self) -> f64 {
        self.height1
    }
    fn height2(&self) -> f64 {
        self.height2
    }
}

impl Calculator for Egg2Calculator {
    fn perform_calculation(&self, depth: f64, velocity: f64) -> Result<f64, CalculationError> {
        self.perform_egg_calculation(depth, velocity)
    }
}
