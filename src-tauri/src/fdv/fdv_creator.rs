use chrono::{NaiveDateTime, ParseError};
use polars::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use thiserror::Error;

use crate::calculations::calculator::{CalculationError, Calculator};
use crate::calculations::circular_calculator::CircularCalculator;
use crate::calculations::egg1_calculator::Egg1Calculator;
use crate::calculations::egg2_calculator::Egg2Calculator;
use crate::calculations::egg2a_calculator::Egg2ACalculator;
use crate::calculations::rectangular_calculator::RectangularCalculator;
use crate::calculations::two_circle_and_rectangle_calculator::TwoCircleAndRectangleCalculator;

#[derive(Error, Debug)]
pub enum FDVFlowCreatorError {
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
    #[error("Calculation error: {0}")]
    CalculationError(#[from] CalculationError),
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),
    #[error("Parse error: {0}")]
    ParseError(#[from] ParseError),
}

pub struct FDVFlowCreator {
    timestamp_col: Option<String>,
    header_lines: Vec<String>,
    start_ts: Option<NaiveDateTime>,
    end_ts: Option<NaiveDateTime>,
    interval: Option<i64>,
    output_file: Option<BufWriter<File>>,
    depth_col: Option<String>,
    velocity_col: Option<String>,
    calculator: Option<Box<dyn Calculator>>,
    df: Option<DataFrame>,
    depth_null_readings: usize,
    velocity_null_readings: usize,
    value_count: usize,
}

impl FDVFlowCreator {
    pub fn new() -> Self {
        FDVFlowCreator {
            header_lines: vec![
                "**DATA_FORMAT:           1,ASCII".to_string(),
                "**IDENTIFIER:            1,SHUTTERT".to_string(),
                "**FIELD:                 3,FLOW,DEPTH,VELOCITY".to_string(),
                "**UNITS:                 3,L/S,MM,M/S".to_string(),
                "**FORMAT:                3,2I5,F5,[5]".to_string(),
                "**RECORD_LENGTH:         I2,75".to_string(),
                "**CONSTANTS:             6,HEIGHT,MIN_VEL,MANHOLE_NO,".to_string(),
                "*+START,END,INTERVAL".to_string(),
                "**C_UNITS:               6,MM,M/S,,GMT,GMT,MIN".to_string(),
                "**C_FORMAT:              10,I5,1X,F5,1X,A20/D10,1X,D10,1X,I2".to_string(),
                "*CSTART".to_string(),
                "  0.200 UNKNOWN".to_string(),
            ],
            timestamp_col: None,
            start_ts: None,
            end_ts: None,
            interval: None,
            output_file: None,
            depth_col: None,
            velocity_col: None,
            calculator: None,
            df: None,
            depth_null_readings: 0,
            velocity_null_readings: 0,
            value_count: 0,
        }
    }
    pub fn set_pipe_dia(&mut self, pipe_dia: f64) {
        self.header_lines[11] = format!("{:7.3} UNKNOWN", pipe_dia);
    }
    pub fn set_site_name(&mut self, site_name: &str) {
        let truncated_name = if site_name.len() > 15 {
            &site_name[..15]
        } else {
            site_name
        };
        self.header_lines[1] = format!(
            "**IDENTIFIER:            1,{}",
            truncated_name.to_uppercase()
        );
    }
    pub fn set_calculator(&mut self, calculator: Box<dyn Calculator>) {
        self.calculator = Some(calculator);
    }

    pub fn set_dataframe(&mut self, df: DataFrame) {
        self.df = Some(df);
    }

    pub fn open_output_file(&mut self, output_file: &str) -> Result<(), FDVFlowCreatorError> {
        let file = File::create(Path::new(output_file))?;
        self.output_file = Some(BufWriter::new(file));
        Ok(())
    }

    pub fn set_starting_time(&mut self, starting_time: &str) -> Result<(), FDVFlowCreatorError> {
        self.start_ts = Some(NaiveDateTime::parse_from_str(
            starting_time,
            "%Y-%m-%d %H:%M:%S",
        )?);
        Ok(())
    }

    pub fn set_ending_time(&mut self, ending_time: &str) -> Result<(), FDVFlowCreatorError> {
        self.end_ts = Some(NaiveDateTime::parse_from_str(
            ending_time,
            "%Y-%m-%d %H:%M:%S",
        )?);
        Ok(())
    }

    pub fn set_interval(&mut self, interval: i64) {
        self.interval = Some(interval);
    }

    fn write_header(&mut self) -> io::Result<()> {
        if let Some(ref mut writer) = self.output_file {
            for line in &self.header_lines {
                writeln!(writer, "{}", line)?;
            }
            let interval_in_minutes = self.interval.unwrap();
            let start_str = self.start_ts.unwrap().format("%Y%m%d%H%M").to_string();
            let end_str = self.end_ts.unwrap().format("%Y%m%d%H%M").to_string();
            writeln!(
                writer,
                "{} {}   {}",
                start_str, end_str, interval_in_minutes
            )?;
            writeln!(writer, "*CEND")?;
        }
        Ok(())
    }

    fn write_tail(&mut self) -> io::Result<()> {
        if let Some(ref mut writer) = self.output_file {
            writeln!(writer, "\n*END")?;
        }
        Ok(())
    }

    fn write_output(&mut self, depth: f64, velocity: f64, result: f64) -> io::Result<()> {
        if let Some(ref mut writer) = self.output_file {
            write!(
                writer,
                "{:5.0}{:5.0}{:5.2}",
                result,
                (depth * 1000.0).round(),
                velocity
            )?;
            if self.value_count % 5 == 0 {
                writeln!(writer)?;
            }
            self.value_count += 1;
        }
        Ok(())
    }

    pub fn process_data(
        &mut self,
        col_names: HashMap<String, String>,
    ) -> Result<(), FDVFlowCreatorError> {
        let depth_col = col_names.get("depth").map(|s| s.as_str()).ok_or_else(|| {
            FDVFlowCreatorError::InvalidParameter("Depth column name not provided".to_string())
        })?;
        let velocity_col = col_names
            .get("velocity")
            .map(|s| s.as_str())
            .ok_or_else(|| {
                FDVFlowCreatorError::InvalidParameter(
                    "Velocity column name not provided".to_string(),
                )
            })?;

        self.value_count = 1;

        let df = self.df.as_mut().ok_or_else(|| {
            FDVFlowCreatorError::InvalidParameter("DataFrame not set".to_string())
        })?;

        if !df.get_column_names().iter().any(|&col| col == depth_col) {
            log::error!(
                "Warning: Depth column '{}' not found. Using 0.0 for all values.",
                depth_col
            );
            df.with_column(Series::new(depth_col.into(), vec![0.0f64; df.height()]))?;
        }

        if !df.get_column_names().iter().any(|&col| col == velocity_col) {
            log::error!(
                "Warning: Velocity column '{}' not found. Using 0.0 for all values.",
                velocity_col
            );
            df.with_column(Series::new(velocity_col.into(), vec![0.0f64; df.height()]))?;
        }

        self.depth_null_readings = df.column(depth_col)?.null_count();
        self.velocity_null_readings = df.column(velocity_col)?.null_count();

        // Handle the Result inside the closure
        df.apply(depth_col, |s| match s.fill_null(FillNullStrategy::Zero) {
            Ok(filled) => filled,
            Err(_) => s.clone(),
        })?;
        df.apply(velocity_col, |s| {
            match s.fill_null(FillNullStrategy::Zero) {
                Ok(filled) => filled,
                Err(_) => s.clone(),
            }
        })?;

        let depth_series = df.column(depth_col)?.clone();
        let velocity_series = df.column(velocity_col)?.clone();

        let calculator = self.calculator.as_ref().ok_or_else(|| {
            FDVFlowCreatorError::InvalidParameter("Calculator not set".to_string())
        })?;

        let depth_values: Vec<f64> = depth_series
            .f64()?
            .into_iter()
            .map(|v| v.unwrap_or(0.0))
            .collect();
        let velocity_values: Vec<f64> = velocity_series
            .f64()?
            .into_iter()
            .map(|v| v.unwrap_or(0.0))
            .collect();

        let results: Vec<_> = depth_values
            .iter()
            .zip(velocity_values.iter())
            .map(|(&depth, &velocity)| {
                let depth =
                    if depth_col.contains("mm") && !depth_col.to_lowercase().contains("level") {
                        depth / 1000.0
                    } else {
                        depth
                    };

                if depth == 0.0 || velocity == 0.0 {
                    Ok((depth, velocity, 0.0))
                } else {
                    calculator
                        .perform_calculation(depth, velocity)
                        .map(|result| (depth, velocity, result))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        for (depth, velocity, result) in results {
            self.write_output(depth, velocity, result)?;
        }

        if self.value_count % 5 != 0 {
            if let Some(ref mut writer) = self.output_file {
                writeln!(writer)?;
            }
        }

        Ok(())
    }
    pub fn get_null_readings(&self) -> (usize, usize) {
        (self.depth_null_readings, self.velocity_null_readings)
    }

    pub fn validate_parameters(&self) -> Result<(), &'static str> {
        if self.start_ts.is_none() {
            return Err("Starting time is not set. Use set_starting_time() method.");
        }
        if self.end_ts.is_none() {
            return Err("Ending time is not set. Use set_ending_time() method.");
        }
        if self.interval.is_none() {
            return Err("Interval is not set. Use set_interval() method.");
        }
        if self.output_file.is_none() {
            return Err("Output file is not set. Use open_output_file() method.");
        }
        if self.df.is_none() || self.df.as_ref().unwrap().height() == 0 {
            return Err("DataFrame is empty or not set.");
        }
        Ok(())
    }

    pub fn set_parameters(
        &mut self,
        df: DataFrame,
        site_name: &str,
        starting_time: &str,
        ending_time: &str,
        interval: i64,
        output_file: &str,
        col_names: &HashMap<String, String>,
        pipe_type: &str,
        pipe_size_param: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.set_dataframe(df);
        self.set_site_name(site_name);
        self.set_starting_time(starting_time)?;
        self.set_ending_time(ending_time)?;
        self.set_interval(interval);
        self.open_output_file(output_file)?;

        if !col_names.contains_key("timestamp")
            || !col_names.contains_key("depth")
            || !col_names.contains_key("velocity")
        {
            return Err("col_names must contain 'timestamp', 'depth', and 'velocity' keys".into());
        }

        self.depth_col = Some(col_names["depth"].clone());
        self.velocity_col = Some(col_names["velocity"].clone());
        self.timestamp_col = Some(col_names["timestamp"].clone());

        self.set_pipe_dia(-1.0);

        let calculator: Box<dyn Calculator> = match pipe_type {
            "Circular" => {
                let pipe_size = pipe_size_param.parse::<f64>()? / 1000.0;
                if pipe_size > 0.0 {
                    self.set_pipe_dia(pipe_size);
                }
                Box::new(CircularCalculator::new(pipe_size / 2.0)?)
            }
            "Rectangular" => {
                let pipe_size = pipe_size_param.parse::<f64>()? / 1000.0;
                if pipe_size > 0.0 {
                    self.set_pipe_dia(pipe_size);
                }
                Box::new(RectangularCalculator::new(pipe_size)?)
            }
            "Egg Type 1" => {
                let egg_params: Vec<f64> = pipe_size_param
                    .split(',')
                    .map(|s| s.parse::<f64>().unwrap())
                    .collect();
                Box::new(Egg1Calculator::new(
                    egg_params[0],
                    egg_params[1],
                    egg_params[2],
                )?)
            }
            "Egg Type 2a" => {
                let egg_params: Vec<f64> = pipe_size_param
                    .split(',')
                    .map(|s| s.parse::<f64>().unwrap())
                    .collect();
                Box::new(Egg2ACalculator::new(
                    egg_params[0],
                    egg_params[1],
                    egg_params[2],
                )?)
            }
            "Egg Type 2" => {
                let egg_height = pipe_size_param.parse::<f64>()?;
                Box::new(Egg2Calculator::new(egg_height)?)
            }
            "Two Circles and a Rectangle" => {
                let params: Vec<f64> = pipe_size_param
                    .split(',')
                    .map(|s| s.parse::<f64>().unwrap())
                    .collect();
                Box::new(TwoCircleAndRectangleCalculator::new(params[1], params[0])?)
            }
            _ => return Err(format!("Unsupported pipe type: {}", pipe_type).into()),
        };

        self.set_calculator(calculator);

        Ok(())
    }

    pub fn create_fdv_flow(&mut self) -> Result<(), FDVFlowCreatorError> {
        // Validate parameters
        self.validate_parameters()
            .map_err(|e| FDVFlowCreatorError::InvalidParameter(e.to_string()))?;

        // Write header
        self.write_header()?;

        // Process data
        let col_names = HashMap::from([
            (
                "timestamp".to_string(),
                self.timestamp_col.clone().unwrap_or_default(),
            ),
            (
                "depth".to_string(),
                self.depth_col.clone().unwrap_or_default(),
            ),
            (
                "velocity".to_string(),
                self.velocity_col.clone().unwrap_or_default(),
            ),
        ]);
        self.process_data(col_names)?;

        // Write tail
        self.write_tail()?;

        // Log success and null readings
        let (depth_null, velocity_null) = self.get_null_readings();
        log::info!(
            "FDV flow creation completed successfully. Null readings: Depth: {}, Velocity: {}",
            depth_null,
            velocity_null
        );

        Ok(())
    }
}
