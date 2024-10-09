use super::calculator::{CalculationError, Calculator};
use super::wetted_area_calculation_helper::WettedAreaCalculationHelper;

pub trait EggCalculator: Calculator {
    fn height(&self) -> f64;
    fn radius1(&self) -> f64;
    fn radius2(&self) -> f64;
    fn radius3(&self) -> f64;
    fn offset(&self) -> f64;
    fn height1(&self) -> f64;
    fn height2(&self) -> f64;

    fn perform_egg_calculation(&self, depth: f64, velocity: f64) -> Result<f64, CalculationError> {
        let [area, _] = WettedAreaCalculationHelper::area(
            self.height(),
            self.radius1(),
            self.radius2(),
            self.radius3(),
            self.height1(),
            self.height2(),
            self.offset(),
            depth,
        );
        let result = velocity * area * 1000.0;
        Ok(result.max(0.0))
    }
}
