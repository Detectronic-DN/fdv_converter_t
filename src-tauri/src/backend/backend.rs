use std::option::Option;
use std::collections::HashMap;
use std::path::PathBuf;
use chrono::Duration;
use polars::prelude::*;
use serde_json::json;
use crate::backend::file_processor::{FileProcessor, ProcessedFileData};
use crate::utils::logger::clear_logs;
use crate::fdv::fdv_creator::FDVFlowCreator;

pub struct CommandHandler {
    filepath: PathBuf,
    site_id: String,
    site_name: String,
    data_frame: Option<DataFrame>,
    start_timestamp: String,
    end_timestamp: String,
    column_mapping: HashMap<String, Vec<(String, usize, Option<String>, Option<String>)>>,
    monitor_type: String,
    interval: Duration,
    gaps: usize,
    time_col: Option<String>,
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
                log::info!("Range: {} to {}",self.start_timestamp, self.end_timestamp);
                log::info!("Monitor type: {}", self.monitor_type);


                Ok(result.to_string())
            },
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

    pub fn update_timestamps(&mut self, start_time: &str, end_time: &str) -> Result<String, String> {

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

                log::info!("Timestamps updated. New range: {} to {}", formatted_start, formatted_end);
                println!("New Updated Df {}", self.data_frame.clone().unwrap().head(Some(5)));
                Ok(result.to_string())
            },
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
        self.time_col = self.column_mapping.get("timestamp")
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
        println!("first 5 rows in a fdv {}", self.data_frame.clone().unwrap().head(Some(5)));
        // Create a new FDVFlowCreator
        let mut fdv_creator = FDVFlowCreator::new();

        // Set up column names
        let mut col_names = HashMap::new();
        col_names.insert("timestamp".to_string(), self.time_col.clone().unwrap_or_default());
        col_names.insert("depth".to_string(), depth_col.to_string());
        col_names.insert("velocity".to_string(), velocity_col.to_string());

        fdv_creator.set_parameters(
            df.clone(),
            &self.site_name,
            &self.start_timestamp,
            &self.end_timestamp,
            self.interval.num_minutes(),
            output_path,
            &col_names,
            pipe_shape,
            pipe_size,
        ).map_err(|e| format!("Error setting FDV flow parameters: {}", e))?;

        fdv_creator.create_fdv_flow()
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
        log::info!("Null readings: Depth: {}, Velocity: {}", depth_null, velocity_null);

        Ok(result.to_string())
    }
}