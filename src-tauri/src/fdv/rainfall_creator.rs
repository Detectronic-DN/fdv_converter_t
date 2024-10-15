use chrono::NaiveDateTime;
use polars::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FDVRainfallCreatorError {
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),
    #[error("Parse error: {0}")]
    ParseError(#[from] chrono::ParseError),
}

pub struct FDVRainfallCreator {
    timestamp_col: Option<String>,
    rainfall_col: Option<String>,
    header_lines: Vec<String>,
    start_ts: Option<NaiveDateTime>,
    end_ts: Option<NaiveDateTime>,
    interval: Option<i64>,
    output_path: Option<BufWriter<File>>,
    df: Option<DataFrame>,
    null_readings: usize,
    value_count: usize,
    drain_size: usize,
    output_buffer: Vec<f64>,
}

impl FDVRainfallCreator {
    pub fn new() -> Self {
        FDVRainfallCreator {
            timestamp_col: None,
            rainfall_col: None,
            header_lines: vec![
                "**DATA_FORMAT:           1,ASCII".to_string(),
                "**IDENTIFIER:            1,SHUTTE".to_string(),
                "**FIELD:                 1,INTENSITY".to_string(),
                "**UNITS:                 1,MM/HR".to_string(),
                "**FORMAT:                2,F15.1,[5]".to_string(),
                "**RECORD_LENGTH:         I2,75".to_string(),
                "**CONSTANTS:             35,LOCATION,0_ANT_RAIN,1_ANT_RAIN,2_ANT_RAIN,"
                    .to_string(),
                "*+                       3_ANT_RAIN,4_ANT_RAIN,5_ANT_RAIN,6_ANT_RAIN,".to_string(),
                "*+                       7_ANT_RAIN,8_ANT_RAIN,9_ANT_RAIN,10_ANT_RAIN,"
                    .to_string(),
                "*+                       11_ANT_RAIN,12_ANT_RAIN,13_ANT_RAIN,14_ANT_RAIN,"
                    .to_string(),
                "*+                       15_ANT_RAIN,16_ANT_RAIN,17_ANT_RAIN,18_ANT_RAIN,"
                    .to_string(),
                "*+                       19_ANT_RAIN,20_ANT_RAIN,21_ANT_RAIN,22_ANT_RAIN,"
                    .to_string(),
                "*+                       23_ANT_RAIN,24_ANT_RAIN,25_ANT_RAIN,26_ANT_RAIN,"
                    .to_string(),
                "*+                       27_ANT_RAIN,28_ANT_RAIN,29_ANT_RAIN,30_ANT_RAIN,"
                    .to_string(),
                "*+                       START,END,INTERVAL".to_string(),
                "**C_UNITS:               35, ,MM,MM,MM,MM,MM,MM,MM,MM,MM,MM,".to_string(),
                "**C_UNITS:               MM,MM,MM,MM,MM,MM,MM,MM,MM,MM,MM,".to_string(),
                "**C_UNITS:               MM,MM,MM,MM,MM,MM,MM,MM,MM,MM,GMT,GMT,MIN".to_string(),
                "**C_FORMAT:              8,A20,F7.2/15F5.1/15F5.1/D10,2X,D10,I4".to_string(),
                "*CSTART".to_string(),
                "UNKNOWN              -1.0 ".to_string(),
                "-1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 "
                    .to_string(),
                "-1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 -1.0 "
                    .to_string(),
            ],
            start_ts: None,
            end_ts: None,
            interval: None,
            output_path: None,
            df: None,
            null_readings: 0,
            value_count: 0,
            drain_size: 10,
            output_buffer: Vec::new(),
        }
    }

    pub fn set_dataframe(&mut self, df: DataFrame) {
        self.df = Some(df);
    }

    pub fn open_output_path(&mut self, output_path: &str) -> Result<(), FDVRainfallCreatorError> {
        let file = File::create(Path::new(output_path))?;
        self.output_path = Some(BufWriter::new(file));
        Ok(())
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

    pub fn set_starting_time(
        &mut self,
        starting_time: &str,
    ) -> Result<(), FDVRainfallCreatorError> {
        self.start_ts = Some(NaiveDateTime::parse_from_str(
            starting_time,
            "%Y-%m-%d %H:%M:%S",
        )?);
        Ok(())
    }

    pub fn set_ending_time(&mut self, ending_time: &str) -> Result<(), FDVRainfallCreatorError> {
        self.end_ts = Some(NaiveDateTime::parse_from_str(
            ending_time,
            "%Y-%m-%d %H:%M:%S",
        )?);
        Ok(())
    }

    pub fn set_interval(&mut self, interval: i64) {
        self.interval = Some(interval);
    }

    fn header(&mut self) -> io::Result<()> {
        if let Some(ref mut writer) = self.output_path {
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
        if let Some(ref mut writer) = self.output_path {
            if (self.value_count - 1) % 5 != 0 {
                writeln!(writer)?;
            }
            writeln!(writer, "\n*END")?;
        }
        Ok(())
    }

    fn drain_output_buffer(&mut self, drain_size: usize) -> io::Result<()> {
        if let Some(ref mut writer) = self.output_path {
            while self.output_buffer.len() > drain_size {
                let sample = self.output_buffer.remove(0);
                write!(writer, "{:15.1}", sample)?;
                if self.value_count % 5 == 0 {
                    writeln!(writer)?;
                }
                self.value_count += 1;
            }
        }
        Ok(())
    }

    fn insert_value(&mut self, sample_value: f64) -> io::Result<()> {
        let mut sample = sample_value;
        if sample > 1.0e-5 {
            let mut count = 0;
            let mut offs = self.output_buffer.len() as i32 - 1;
            let mut divisor = 1.0;
            while offs >= 0 && count < 4 {
                let sa = self.output_buffer[offs as usize];
                if sa >= 1.0e-5 {
                    break;
                }
                divisor += 1.0;
                count += 1;
                offs -= 1;
            }
            offs += 1;
            if count > 0 && sample > 6.0 {
                sample = 6.0 / (divisor - 1.0);
                while offs < self.output_buffer.len() as i32 {
                    self.output_buffer[offs as usize] = sample;
                    offs += 1;
                }
                sample = sample_value - 6.0;
            } else {
                sample /= divisor;
                while offs < self.output_buffer.len() as i32 {
                    self.output_buffer[offs as usize] = sample;
                    offs += 1;
                }
            }
        }
        self.output_buffer.push(sample);
        if self.output_buffer.len() >= 10 {
            self.drain_output_buffer(self.drain_size)?;
        }
        Ok(())
    }

    pub fn process_data(
        &mut self,
        col_names: HashMap<String, String>,
    ) -> Result<(), FDVRainfallCreatorError> {
        let rainfall_col = col_names.get("rainfall").ok_or_else(|| {
            FDVRainfallCreatorError::InvalidParameter(
                "Rainfall column name not provided".to_string(),
            )
        })?;

        self.value_count = 1;

        let df = self.df.as_mut().ok_or_else(|| {
            FDVRainfallCreatorError::InvalidParameter("DataFrame not set".to_string())
        })?;

        self.null_readings = df.column(rainfall_col)?.null_count();

        let rainfall_series = df.column(rainfall_col)?.clone();
        let rainfall_values: Vec<f64> = rainfall_series
            .f64()?
            .into_iter()
            .map(|v| v.unwrap_or(0.0))
            .collect();

        for value in rainfall_values {
            self.insert_value(value)?;
        }

        self.drain_output_buffer(0)?;

        Ok(())
    }

    pub fn get_null_readings(&self) -> usize {
        self.null_readings
    }

    pub fn validate_params(&self) -> Result<(), &'static str> {
        if self.start_ts.is_none() {
            return Err("Starting time is not set. Use set_starting_time() method.");
        }
        if self.end_ts.is_none() {
            return Err("Ending time is not set. Use set_ending_time() method.");
        }
        if self.interval.is_none() {
            return Err("Interval is not set. Use set_interval() method.");
        }
        if self.output_path.is_none() {
            return Err("Output file is not set. Use open_output_path() method.");
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
        output_path: &str,
        col_names: &HashMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.set_dataframe(df);
        self.set_site_name(site_name);
        self.set_starting_time(starting_time)?;
        self.set_ending_time(ending_time)?;
        self.set_interval(interval);
        self.open_output_path(output_path)?;

        if !col_names.contains_key("timestamp") || !col_names.contains_key("rainfall") {
            return Err("col_names must contain 'timestamp' and 'rainfall' keys".into());
        }

        self.rainfall_col = Some(col_names["rainfall"].clone());
        self.timestamp_col = Some(col_names["timestamp"].clone());

        Ok(())
    }

    pub fn create_fdv_rainfall(&mut self) -> Result<(), FDVRainfallCreatorError> {
        self.validate_params()
            .map_err(|e| FDVRainfallCreatorError::InvalidParameter(e.to_string()))?;

        self.header()?;

        let col_names = HashMap::from([
            (
                "timestamp".to_string(),
                self.timestamp_col.clone().unwrap_or_default(),
            ),
            (
                "rainfall".to_string(),
                self.rainfall_col.clone().unwrap_or_default(),
            ),
        ]);
        self.process_data(col_names)?;

        self.write_tail()?;

        log::info!(
            "FDV rainfall creation completed successfully. Null readings: {}",
            self.get_null_readings()
        );

        Ok(())
    }
}
