use std::error::Error;
use std::fmt;

// Define the Calculator trait (equivalent to Python's ABC)
pub trait Calculator {
    fn perform_calculation(&self, depth: f64, velocity: f64) -> Result<f64, CalculationError>;
}

// Custom error type
#[derive(Debug)]
pub struct CalculationError {
    message: String,
}

impl CalculationError {
    pub fn new(message: &str) -> Self {
        CalculationError {
            message: message.to_string(),
        }
    }
}

impl fmt::Display for CalculationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Calculation Error: {}", self.message)
    }
}

impl Error for CalculationError {}
