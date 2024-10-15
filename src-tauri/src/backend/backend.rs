use crate::backend::batch_processing::BatchProcessor;
use crate::backend::file_processor::{FileProcessor, ProcessedFileData};
use crate::backend::interim_reports::InterimReportGenerator;
use crate::calculations::r3_calculator::r3_calculator;
use crate::fdv::fdv_creator::FDVFlowCreator;
use crate::fdv::rainfall_creator::FDVRainfallCreator;
use crate::utils::logger::clear_logs;
use chrono::Duration;
use polars::prelude::*;
use rust_xlsxwriter::{Workbook, Worksheet};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::error::Error;
use std::option::Option;
use std::path::{Path, PathBuf};
use std::time::Instant;

pub struct CommandHandler {
    filepath: PathBuf,
    site_id: String,
    site_name: String,
    pub(crate) data_frame: Option<DataFrame>,
    start_timestamp: String,
    end_timestamp: String,
    pub(crate) column_mapping:
        HashMap<String, Vec<(String, usize, Option<String>, Option<String>)>>,
    pub(crate) monitor_type: String,
    pub(crate) interval: Duration,
    gaps: usize,
    pub(crate) time_col: Option<String>,
}

impl CommandHandler {
    pub fn new() -> CommandHandler {
        CommandHandler {
            filepath: PathBuf::new(),
            site_id: String::new(),
            site_name: String::new(),
            data_frame: None,
            start_timestamp: String::new(),
            end_timestamp: String::new(),
            column_mapping: HashMap::new(),
            monitor_type: String::new(),
            interval: Duration::seconds(0),
            gaps: 0,
            time_col: None,
        }
    }

    pub fn process_file(&mut self, file_path: &str) -> Result<String, String> {
        self.filepath = PathBuf::from(file_path);
        let mut file_processor: FileProcessor = FileProcessor::new(None);
        match file_processor.process_file(&file_path) {
            Ok(processed_data) => {
                self.update_from_processed_data(processed_data);

                let result = json!({
                    "success": true,
                    "message": "File processed successfully",
                    "columnMapping": self.column_mapping,
                    "monitorType": self.monitor_type,
                    "startTimestamp": self.start_timestamp,
                    "endTimestamp": self.end_timestamp,
                    "interval": self.interval.num_seconds(),
                    "siteId": self.site_id,
                    "siteName": self.site_name,
                    "gaps": self.gaps,
                });

                log::info!("File processed successfully.");
                log::info!("Gaps: {}", self.gaps);
                log::info!("Range: {} to {}", self.start_timestamp, self.end_timestamp);
                log::info!("Monitor type: {}", self.monitor_type);

                Ok(result.to_string())
            }
            Err(e) => {
                let error_message = format!("Error processing file: {}", e);
                log::error!("{}", error_message);
                Err(error_message)
            }
        }
    }

    fn format_timestamp(&self, timestamp: &str) -> Result<String, String> {
        // Parse the input timestamp
        let dt = chrono::NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%dT%H:%M")
            .or_else(|_| chrono::NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S"))
            .map_err(|e| format!("Failed to parse timestamp: {}", e))?;

        // Format the timestamp in the required format
        Ok(dt.format("%Y-%m-%d %H:%M:%S").to_string())
    }

    pub fn update_timestamps(
        &mut self,
        start_time: &str,
        end_time: &str,
    ) -> Result<String, String> {
        let formatted_start = self.format_timestamp(start_time)?;
        let formatted_end = self.format_timestamp(end_time)?;

        let mut file_processor = FileProcessor::new(None);
        file_processor.df = self.data_frame.clone();
        file_processor.time_col = self.time_col.clone();
        file_processor.interval = Some(self.interval);

        match file_processor.update_timestamps(&formatted_start, &formatted_end) {
            Ok(updated_data) => {
                self.start_timestamp = updated_data.start_timestamp;
                self.end_timestamp = updated_data.end_timestamp;
                self.interval = updated_data.interval;
                self.data_frame = file_processor.df;

                let result = json!({
                    "success": true,
                    "message": "Timestamps updated successfully",
                    "startTimestamp": self.start_timestamp,
                    "endTimestamp": self.end_timestamp,
                    "interval": self.interval.num_seconds(),
                    "rowCount": updated_data.row_count,
                });

                log::info!(
                    "Timestamps updated. New range: {} to {}",
                    formatted_start,
                    formatted_end
                );
                Ok(result.to_string())
            }
            Err(e) => {
                let error_message = format!("Error updating timestamps: {}", e);
                log::error!("{}", error_message);
                Err(error_message)
            }
        }
    }

    fn update_from_processed_data(&mut self, processed_data: ProcessedFileData) {
        self.site_id = processed_data.site_id;
        self.site_name = processed_data.site_name;
        self.data_frame = Some(processed_data.df);
        self.start_timestamp = processed_data.start_timestamp;
        self.end_timestamp = processed_data.end_timestamp;
        self.column_mapping = processed_data.column_mapping;
        self.monitor_type = processed_data.monitor_type;
        self.interval = processed_data.interval;
        self.gaps = processed_data.gaps_filled;
        self.time_col = self
            .column_mapping
            .get("timestamp")
            .and_then(|v| v.first())
            .map(|(name, _, _, _)| name.clone());
    }

    pub fn update_site_id(&mut self, site_id: String) -> Result<String, String> {
        self.site_id = site_id;
        let result = json!({
            "success": true,
            "message": "Site ID updated successfully",
            "siteId": self.site_id,
        });
        log::info!("Site ID updated. {}", self.site_id);
        Ok(result.to_string())
    }

    pub fn update_site_name(&mut self, site_name: String) -> Result<String, String> {
        self.site_name = site_name;
        let result = json!({
            "success": true,
            "message": "Site name updated successfully",
            "siteName": self.site_name,
        });
        log::info!("Site name updated. {}", self.site_name);
        Ok(result.to_string())
    }

    pub fn reset(&mut self) {
        *self = CommandHandler::new();
        clear_logs();
    }

    pub fn create_fdv_flow(
        &mut self,
        output_path: &str,
        depth_col: &str,
        velocity_col: &str,
        pipe_shape: &str,
        pipe_size: &str,
    ) -> Result<String, String> {
        let df = self.data_frame.as_ref().ok_or("No data frame available")?;
        // Create a new FDVFlowCreator
        let mut fdv_creator = FDVFlowCreator::new();

        // Set up column names
        let mut col_names = HashMap::new();
        col_names.insert(
            "timestamp".to_string(),
            self.time_col.clone().unwrap_or_default(),
        );
        col_names.insert("depth".to_string(), depth_col.to_string());
        col_names.insert("velocity".to_string(), velocity_col.to_string());

        fdv_creator
            .set_parameters(
                df.clone(),
                &self.site_name,
                &self.start_timestamp,
                &self.end_timestamp,
                self.interval.num_minutes(),
                output_path,
                &col_names,
                pipe_shape,
                pipe_size,
            )
            .map_err(|e| format!("Error setting FDV flow parameters: {}", e))?;

        fdv_creator
            .create_fdv_flow()
            .map_err(|e| format!("Error creating FDV flow: {}", e))?;

        let (depth_null, velocity_null) = fdv_creator.get_null_readings();

        let result = json!({
            "success": true,
            "message": "FDV flow creation initiated",
            "outputPath": output_path,
            "depthColumn": depth_col,
            "velocityColumn": velocity_col,
            "pipeShape": pipe_shape,
            "pipeSize": pipe_size,
            "nullReadings": {
                "depth": depth_null,
                "velocity": velocity_null
            }
        });

        log::info!("FDV flow created successfully. Output: {}", output_path);
        log::info!(
            "Null readings: Depth: {}, Velocity: {}",
            depth_null,
            velocity_null
        );

        Ok(result.to_string())
    }

    pub fn create_rainfall(
        &mut self,
        output_path: &str,
        rainfall_col: &str,
    ) -> Result<String, String> {
        let df = self.data_frame.as_ref().ok_or("No data frame available")?;
        let mut rainfall_creator = FDVRainfallCreator::new();
        let mut col_names = HashMap::new();
        col_names.insert(
            "timestamp".to_string(),
            self.time_col.clone().unwrap_or_default(),
        );
        col_names.insert("rainfall".to_string(), rainfall_col.to_string());

        rainfall_creator
            .set_parameters(
                df.clone(),
                &self.site_name,
                &self.start_timestamp,
                &self.end_timestamp,
                self.interval.num_minutes(),
                output_path,
                &col_names,
            )
            .map_err(|e| format!("Error setting Rainfall parameter: {}", e))?;

        rainfall_creator
            .create_fdv_rainfall()
            .map_err(|e| format!("Error creating FDV flow: {}", e))?;

        let null_readings = rainfall_creator.get_null_readings();

        let result = json!({
            "success": true,
            "message": "Rainfall creation initiated",
            "outputPath": output_path,
            "rainfallColumn": rainfall_col,
            "nullReadings": null_readings
        });

        log::info!("Rainfall creation successfully. Output: {}", output_path);
        log::info!("Null readings: {}", null_readings);

        Ok(result.to_string())
    }
    pub fn calculate_r3(&self, width: f64, height: f64, egg_form: &str) -> f64 {
        let egg_form_value = match egg_form {
            "Egg Type 1" => 1,
            "Egg Type 2" => 2,
            _ => {
                log::error!("Unknown egg form: {}", egg_form);
                return -1.0;
            }
        };

        match r3_calculator(width, height, egg_form_value) {
            Ok(r3_value) => {
                log::info!("R3 value calculated successfully: {}", r3_value);
                r3_value
            }
            Err(e) => {
                log::error!("Error calculating R3 value: {:?}", e);
                -1.0
            }
        }
    }

    pub fn run_batch_process(
        &self,
        file_infos: Vec<Value>,
        output_dir: &Path,
    ) -> Result<(), Box<dyn Error>> {
        let mut batch_processor = BatchProcessor::new();
        let start_time = Instant::now();

        log::info!("Starting batch processing {} files...", file_infos.len());

        match batch_processor.process_convert_and_zip(file_infos, output_dir) {
            Ok(zip_path) => {
                let duration = start_time.elapsed();
                log::info!(
                    "Batch processing and zipping completed successfully in {:?}.",
                    duration
                );
                log::info!("Output zip file: {:?}", zip_path);
            }
            Err(e) => {
                log::error!("Error during processing, conversion, or zipping: {}", e);
                return Err(Box::new(e));
            }
        }

        Ok(())
    }
    pub fn generate_interim_reports(
        &self,
    ) -> Result<(DataFrame, DataFrame, DataFrame), Box<dyn Error>> {
        let mut interim_report_generator = InterimReportGenerator::new(self).unwrap();
        interim_report_generator.generate_report()
    }

    pub fn generate_rainfall_totals(&self) -> Result<(DataFrame, DataFrame), Box<dyn Error>> {
        let interim_report_generator = InterimReportGenerator::new(self).unwrap();
        interim_report_generator.generate_rainfall_totals()
    }

    fn write_df_to_worksheet(
        df: &DataFrame,
        worksheet: &mut Worksheet,
    ) -> Result<(), Box<dyn Error>> {
        // Write headers
        for (col, name) in df.get_column_names().iter().enumerate() {
            worksheet.write_string(0, col as u16, &name.to_string())?;
        }

        // Write data
        for (row, series) in df.iter().enumerate() {
            for (col, value) in series.iter().enumerate() {
                match value {
                    AnyValue::Float64(f) => {
                        worksheet.write_number(row as u32 + 1, col as u16, f)?
                    }
                    AnyValue::Float32(f) => {
                        worksheet.write_number(row as u32 + 1, col as u16, f as f64)?
                    }
                    AnyValue::Int64(i) => {
                        worksheet.write_number(row as u32 + 1, col as u16, i as i32)?
                    }
                    AnyValue::Int32(i) => worksheet.write_number(row as u32 + 1, col as u16, i)?,
                    AnyValue::UInt64(u) => {
                        worksheet.write_number(row as u32 + 1, col as u16, u as u32)?
                    }
                    AnyValue::UInt32(u) => worksheet.write_number(row as u32 + 1, col as u16, u)?,
                    AnyValue::Int16(i) => worksheet.write_number(row as u32 + 1, col as u16, i)?,
                    AnyValue::UInt16(u) => worksheet.write_number(row as u32 + 1, col as u16, u)?,
                    AnyValue::Int8(i) => worksheet.write_number(row as u32 + 1, col as u16, i)?,
                    AnyValue::UInt8(u) => worksheet.write_number(row as u32 + 1, col as u16, u)?,
                    AnyValue::String(s) => worksheet.write_string(row as u32 + 1, col as u16, s)?,
                    AnyValue::Null => worksheet.write_string(row as u32 + 1, col as u16, "")?,
                    _ => worksheet.write_string(row as u32 + 1, col as u16, &value.to_string())?,
                };
            }
        }

        Ok(())
    }

    pub fn save_interim_reports_to_excel(&self, file_path: &str) -> Result<(), Box<dyn Error>> {
        // Create a new workbook
        let mut workbook = Workbook::new();

        // Generate interim reports
        let (summaries, complete_data, daily_summary) = self.generate_interim_reports()?;

        // Write each DataFrame to a separate worksheet
        let mut worksheet = workbook.add_worksheet();
        worksheet.set_name("Summaries")?;
        Self::write_df_to_worksheet(&summaries, &mut worksheet)?;

        let mut worksheet = workbook.add_worksheet();
        worksheet.set_name("Complete Data")?;
        Self::write_df_to_worksheet(&complete_data, &mut worksheet)?;

        let mut worksheet = workbook.add_worksheet();
        worksheet.set_name("Daily Summary")?;
        Self::write_df_to_worksheet(&daily_summary, &mut worksheet)?;

        // Save the workbook
        workbook.save(file_path)?;

        log::info!(
            "Interim reports Excel file saved successfully: {}",
            file_path
        );
        Ok(())
    }

    pub fn save_rainfall_totals_to_excel(&self, file_path: &str) -> Result<(), Box<dyn Error>> {
        if self.monitor_type != "Rainfall" {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Rainfall totals are only available for Rainfall monitor type",
            )));
        }

        // Create a new workbook
        let mut workbook = Workbook::new();

        // Generate rainfall totals
        let (daily_totals, weekly_totals) = self.generate_rainfall_totals()?;

        // Write each DataFrame to a separate worksheet
        let mut worksheet = workbook.add_worksheet();
        worksheet.set_name("Daily Rainfall Totals")?;
        Self::write_df_to_worksheet(&daily_totals, &mut worksheet)?;

        let mut worksheet = workbook.add_worksheet();
        worksheet.set_name("Weekly Rainfall Totals")?;
        Self::write_df_to_worksheet(&weekly_totals, &mut worksheet)?;

        // Save the workbook
        workbook.save(file_path)?;

        log::info!(
            "Rainfall totals Excel file saved successfully: {}",
            file_path
        );
        Ok(())
    }
}
