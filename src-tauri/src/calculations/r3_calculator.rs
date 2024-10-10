use std::f64;

#[derive(Debug)]
pub enum R3CalculatorError {
    MathDomainError,
    ConvergenceError,
}

pub fn r3_calculator(w: f64, h: f64, egg_form: i32) -> Result<f64, R3CalculatorError> {
    let max_iterations: i32 = 1000;
    let precision: f64 = 1e-5;
    let r2: f64 = w / 2.0;

    let r1: f64 = (h - w) / if egg_form == 1 { 2.0 } else { 4.0 };
    let h2: f64 = h - r2;
    let mut r3: f64 = h;

    for _ in 0..max_iterations {
        let offset: f64 = r3 - r2;
        let square_term: f64 = (r3 - r1).powi(2) - (h2 - r1).powi(2);

        if square_term < 0.0 {
            return Err(R3CalculatorError::MathDomainError);
        }

        let offset_a: f64 = square_term.sqrt();
        let diff = offset - offset_a;

        if diff.abs() <= precision {
            return Ok(r3);
        }

        r3 += diff / 10.0;
    }

    Err(R3CalculatorError::ConvergenceError)
}
