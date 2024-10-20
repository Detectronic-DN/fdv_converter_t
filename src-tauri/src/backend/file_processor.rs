use crate::backend::site_info::SiteInfo;
use calamine::{ open_workbook, Reader, Xlsx };
use chrono::{ Duration, NaiveDate, NaiveDateTime, NaiveTime };
use csv::ReaderBuilder;
use log::{ error, info };
use polars::prelude::*;
use rayon::prelude::*;
use regex::Regex;
use serde::{ Deserialize, Serialize };
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileData {
    pub headers: Vec<String>,
    pub data: Vec<Vec<String>>,
}
pub struct FileProcessor {
    timestamp_keywords: Vec<String>,
    pub(crate) time_col: Option<String>,
    start_timestamp: Option<String>,
    end_timestamp: Option<String>,
    pub df: Option<DataFrame>,
    pub(crate) interval: Option<Duration>,
    column_patterns: HashMap<String, Regex>,
    pub(crate) monitor_type: String,
    site_info: SiteInfo,
}

pub struct ProcessedFileData {
    pub df: DataFrame,
    pub start_timestamp: String,
    pub end_timestamp: String,
    pub gaps_filled: usize,
    pub interval: Duration,
    pub column_mapping: HashMap<String, Vec<(String, usize, Option<String>, Option<String>)>>,
    pub monitor_type: String,
    pub site_id: String,
    pub site_name: String,
}

pub struct UpdatedTimestampData {
    pub start_timestamp: String,
    pub end_timestamp: String,
    pub interval: Duration,
    pub row_count: usize,
}

#[derive(Error, Debug)]
pub enum FileProcessorError {
    #[error("File not found: {0}")] FileNotFound(String),
    #[error("Unsupported file format: {0}")] UnsupportedFileFormat(String),
    #[error("FileData is empty")]
    EmptyFileData,
    #[error("Timestamp column not found")]
    TimestampColumnNotFound,
    #[error("Unable to identify timestamp format")]
    TimestampFormatNotIdentified,
    #[error("No sheets found in Excel file")]
    SheetNotFound,
    #[error("Parse error: {0}")] ParseError(String),
    #[error("IO error: {0}")] IoError(#[from] std::io::Error),
    #[error("CSV error: {0}")] CsvError(#[from] csv::Error),
    #[error("Polars error: {0}")] PolarsError(#[from] PolarsError),
}

impl FileProcessor {
    pub fn new(timestamp_keywords: Option<Vec<String>>) -> Self {
        let column_patterns = HashMap::from([
            ("depth".to_string(), Regex::new(r"(?i)(\d+)_(\d+)\|.*(Depth|Level)\|(m|mm)").unwrap()),
            ("flow".to_string(), Regex::new(r"(?i)(\d+)_(\d+)\|.*Flow\|(l/s|m3/s)").unwrap()),
            ("velocity".to_string(), Regex::new(r"(?i)(\d+)_(\d+)\|.*Velocity\|m/s").unwrap()),
            ("rainfall".to_string(), Regex::new(r"(?i)(\d+)_(\d+)\|.*Rainfall\|mm").unwrap()),
        ]);

        FileProcessor {
            timestamp_keywords: timestamp_keywords.unwrap_or_else(|| {
                vec![
                    "timestamp".to_string(),
                    "time stamp".to_string(),
                    "time".to_string(),
                    "date".to_string(),
                    "datetime".to_string()
                ]
            }),
            time_col: None,
            start_timestamp: None,
            end_timestamp: None,
            df: None,
            interval: None,
            column_patterns,
            monitor_type: "Unknown".to_string(),
            site_info: SiteInfo::new(),
        }
    }

    pub fn read_file(&mut self, file_path: &str) -> Result<FileData, FileProcessorError> {
        let path = Path::new(file_path);
        let extension = path
            .extension()
            .and_then(|ext| ext.to_str())
            .ok_or_else(|| FileProcessorError::UnsupportedFileFormat("Unknown".to_string()))?;

        match extension.to_lowercase().as_str() {
            "xlsx" => self.read_excel(file_path),
            "csv" => self.read_csv(file_path),
            _ => {
                error!("Unsupported file format: {}", extension);
                Err(FileProcessorError::UnsupportedFileFormat(extension.to_string()))
            }
        }
    }

    fn read_excel(&mut self, file_path: &str) -> Result<FileData, FileProcessorError> {
        info!("Reading Excel file: {}", file_path);

        let mut workbook: Xlsx<_> = open_workbook(file_path).map_err(|_|
            FileProcessorError::FileNotFound(file_path.to_string())
        )?;
        let sheet_name = workbook
            .sheet_names()
            .get(0)
            .ok_or(FileProcessorError::SheetNotFound)?
            .clone();
        let range = workbook.worksheet_range(&sheet_name);
        let mut headers = Vec::new();
        let mut data = Vec::new();
        for (row_index, row) in range.unwrap().rows().enumerate() {
            if row_index == 0 {
                headers = row
                    .iter()
                    .map(|cell| cell.to_string())
                    .collect();
            } else {
                let row_data: Vec<String> = row
                    .iter()
                    .map(|cell| cell.to_string())
                    .collect();
                data.push(row_data);
            }
        }
        if data.is_empty() {
            error!("Excel file is empty: {}", file_path);
            return Err(FileProcessorError::EmptyFileData);
        }
        let mut file_data = FileData { headers, data };
        self.convert_excel_timestamp(&mut file_data)?;

        Ok(file_data)
    }

    fn read_csv(&self, file_path: &str) -> Result<FileData, FileProcessorError> {
        info!("Reading CSV file: {}", file_path);

        let mut file = File::open(file_path)?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;

        let mut reader = ReaderBuilder::new().has_headers(true).from_reader(content.as_bytes());

        let headers = reader
            .headers()?
            .iter()
            .map(|s| s.to_string())
            .collect();

        let data: Vec<Vec<String>> = reader
            .records()
            .map(|record|
                record.map(|r|
                    r
                        .iter()
                        .map(|s| s.to_string())
                        .collect()
                )
            )
            .collect::<Result<_, _>>()?;

        if data.is_empty() {
            error!("CSV file is empty: {}", file_path);
            return Err(FileProcessorError::EmptyFileData);
        }

        Ok(FileData { headers, data })
    }

    pub fn convert_excel_timestamp(
        &mut self,
        file_data: &mut FileData
    ) -> Result<(), FileProcessorError> {
        let timestamp_column = self.identify_timestamp_column(file_data)?;
        let column_index = file_data.headers
            .iter()
            .position(|h| h == &timestamp_column)
            .ok_or(FileProcessorError::TimestampColumnNotFound)?;

        let excel_epoch = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(1899, 12, 30).unwrap(),
            NaiveTime::from_hms_opt(0, 0, 0).unwrap()
        );

        file_data.data.par_iter_mut().for_each(|row| {
            if let Some(timestamp) = row.get_mut(column_index) {
                if let Ok(excel_date) = timestamp.parse::<f64>() {
                    let days = excel_date.trunc() as i64;
                    let seconds = (excel_date.fract() * 86400.0).round() as i64;
                    let datetime = excel_epoch + Duration::days(days) + Duration::seconds(seconds);
                    *timestamp = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
                }
            }
        });
        Ok(())
    }

    pub fn identify_timestamp_column(
        &self,
        file_data: &FileData
    ) -> Result<String, FileProcessorError> {
        file_data.headers
            .iter()
            .find(|&col| {
                self.timestamp_keywords.iter().any(|keyword| col.to_lowercase().contains(keyword))
            })
            .cloned()
            .ok_or(FileProcessorError::TimestampColumnNotFound)
    }

    pub fn identify_timestamp_format(
        &self,
        file_data: &FileData,
        timestamp_column: &str
    ) -> Result<String, FileProcessorError> {
        let timestamp_formats = vec![
            "%d/%m/%Y %H:%M",
            "%m/%d/%Y %H:%M",
            "%d-%m-%Y %H:%M:%S",
            "%d-%m-%Y %H:%M",
            "%Y%m%d%H%M%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S"
        ];
        let column_index = file_data.headers
            .iter()
            .position(|h| h == timestamp_column)
            .ok_or(FileProcessorError::TimestampColumnNotFound)?;
        let mut format_counts = HashMap::new();
        let max_rows_to_check = (100).min(file_data.data.len());
        for row in file_data.data.iter().take(max_rows_to_check) {
            if let Some(timestamp) = row.get(column_index) {
                for format in &timestamp_formats {
                    if NaiveDateTime::parse_from_str(timestamp, format).is_ok() {
                        *format_counts.entry(format).or_insert(0) += 1;
                        break;
                    }
                }
            }
        }
        format_counts
            .into_iter()
            .max_by_key(|&(_, count)| count)
            .map(|(format, _)| format.to_string())
            .ok_or(FileProcessorError::TimestampFormatNotIdentified)
    }

    pub fn parse_dates(
        &self,
        file_data: &mut FileData,
        timestamp_column: &str,
        format: &str
    ) -> Result<(), FileProcessorError> {
        let column_index = file_data.headers
            .iter()
            .position(|h| h == timestamp_column)
            .ok_or(FileProcessorError::TimestampColumnNotFound)?;
        file_data.data.par_iter_mut().for_each(|row| {
            if let Some(timestamp) = row.get_mut(column_index) {
                *timestamp = NaiveDateTime::parse_from_str(timestamp, format)
                    .map(|parsed_date| parsed_date.format("%Y-%m-%d %H:%M:%S").to_string())
                    .unwrap_or_else(|_| "Invalid Date".to_string());
            }
        });
        Ok(())
    }

    fn get_parsed_timestamps(
        &self,
        file_data: &FileData,
        timestamp_column: &str,
        format: &str
    ) -> Result<Vec<NaiveDateTime>, FileProcessorError> {
        let column_index = file_data.headers
            .iter()
            .position(|h| h == timestamp_column)
            .ok_or(FileProcessorError::TimestampColumnNotFound)?;
        let timestamps: Vec<NaiveDateTime> = file_data.data
            .iter()
            .filter_map(|row| {
                row.get(column_index).and_then(|timestamp|
                    NaiveDateTime::parse_from_str(timestamp, format).ok()
                )
            })
            .collect();
        if timestamps.is_empty() {
            return Err(FileProcessorError::ParseError("No valid timestamps found".to_string()));
        }
        Ok(timestamps)
    }

    pub fn get_start_end_timestamps(
        &self,
        file_data: &FileData,
        timestamp_column: &str,
        format: &str
    ) -> Result<(String, String), FileProcessorError> {
        let mut timestamps = self.get_parsed_timestamps(file_data, timestamp_column, format)?;
        timestamps.sort_unstable();
        let start = timestamps
            .first()
            .ok_or(FileProcessorError::ParseError("Failed to get start timestamp".to_string()))?;
        let end = timestamps
            .last()
            .ok_or(FileProcessorError::ParseError("Failed to get end timestamp".to_string()))?;
        Ok((
            start.format("%Y-%m-%d %H:%M:%S").to_string(),
            end.format("%Y-%m-%d %H:%M:%S").to_string(),
        ))
    }

    pub fn calculate_interval(
        &self,
        file_data: &FileData,
        timestamp_column: &str,
        format: &str
    ) -> Result<Duration, FileProcessorError> {
        let mut timestamps = self.get_parsed_timestamps(file_data, timestamp_column, format)?;
        timestamps.sort_unstable();
        let mut intervals = HashMap::new();
        for window in timestamps.windows(2) {
            if let [prev, next] = window {
                let diff = *next - *prev;
                *intervals.entry(diff).or_insert(0) += 1;
            }
        }
        intervals
            .into_iter()
            .max_by_key(|&(_, count)| count)
            .map(|(interval, _)| interval)
            .ok_or_else(|| {
                FileProcessorError::ParseError("Could not determine a mode interval".to_string())
            })
    }

    pub fn create_timestamp_series(
        &mut self,
        file_data: &FileData,
        timestamp_column: &str,
        format: &str
    ) -> Result<(FileData, usize), FileProcessorError> {
        let (start_str, end_str) = self.get_start_end_timestamps(
            file_data,
            timestamp_column,
            format
        )?;
        let start = NaiveDateTime::parse_from_str(&start_str, "%Y-%m-%d %H:%M:%S").map_err(|_| {
            FileProcessorError::ParseError("Failed to parse start timestamp".to_string())
        })?;
        let end = NaiveDateTime::parse_from_str(&end_str, "%Y-%m-%d %H:%M:%S").map_err(|_| {
            FileProcessorError::ParseError("Failed to parse end timestamp".to_string())
        })?;
        let interval = self.calculate_interval(file_data, timestamp_column, format)?;
        self.interval = Some(interval.clone());
        let timestamp_index = file_data.headers
            .iter()
            .position(|h| h == timestamp_column)
            .ok_or(FileProcessorError::TimestampColumnNotFound)?;
        let mut data_map: HashMap<String, Vec<String>> = HashMap::new();
        for row in &file_data.data {
            if let Some(timestamp) = row.get(timestamp_index) {
                let parsed_timestamp = NaiveDateTime::parse_from_str(timestamp, format).map_err(
                    |_| {
                        FileProcessorError::ParseError(
                            format!("Failed to parse timestamp: {}", timestamp)
                        )
                    }
                )?;
                let formatted_timestamp = parsed_timestamp.format("%Y-%m-%d %H:%M:%S").to_string();
                data_map.insert(formatted_timestamp, row.clone());
            }
        }
        let mut new_data: Vec<Vec<String>> = Vec::new();
        let mut gap_count = 0;
        let mut current = start;
        while current <= end {
            let timestamp = current.format("%Y-%m-%d %H:%M:%S").to_string();
            if let Some(existing_row) = data_map.get(&timestamp) {
                new_data.push(existing_row.clone());
            } else {
                let mut empty_row = vec![timestamp];
                empty_row.extend(vec!["".to_string(); file_data.headers.len() - 1]);
                new_data.push(empty_row);
                gap_count += 1;
            }
            current += interval;
        }
        let new_file_data = FileData {
            headers: file_data.headers.clone(),
            data: new_data,
        };
        Ok((new_file_data, gap_count))
    }

    fn extract_columns(
        &self,
        pattern: &Regex,
        df_columns: &[String]
    ) -> Vec<(String, usize, Option<String>, Option<String>)> {
        df_columns
            .iter()
            .enumerate()
            .filter_map(|(index, col)| {
                pattern
                    .captures(col)
                    .map(|caps| {
                        (
                            col.to_string(),
                            index,
                            caps.get(1).map(|m| m.as_str().to_string()),
                            caps.get(2).map(|m| m.as_str().to_string()),
                        )
                    })
            })
            .collect()
    }

    pub fn get_column_names_and_indices(
        &mut self,
        file_name: &str
    ) -> Result<
        HashMap<String, Vec<(String, usize, Option<String>, Option<String>)>>,
        FileProcessorError
    > {
        let df = self.df
            .as_ref()
            .ok_or(FileProcessorError::ParseError("DataFrame not available".to_string()))?;
        let df_columns: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|&s| s.to_string())
            .collect();
        let mut column_mapping: HashMap<
            String,
            Vec<(String, usize, Option<String>, Option<String>)>
        > = HashMap::new();
        // Extract timestamp column
        if let Some(timestamp_col) = self.time_col.as_ref() {
            if let Some(index) = df_columns.iter().position(|c| c == timestamp_col) {
                column_mapping.insert(
                    "timestamp".to_string(),
                    vec![(timestamp_col.clone(), index, None, None)]
                );
            }
        }
        // Extract other columns
        for (col_type, pattern) in &self.column_patterns {
            let cols = self.extract_columns(pattern, &df_columns);
            if !cols.is_empty() {
                column_mapping.insert(col_type.clone(), cols);
            }
        }
        self.determine_monitor_type(file_name, &column_mapping);
        Ok(column_mapping)
    }

    fn determine_monitor_type(
        &mut self,
        file_name: &str,
        column_mapping: &HashMap<String, Vec<(String, usize, Option<String>, Option<String>)>>
    ) {
        self.site_info.determine_monitor_type(file_name, column_mapping);
        self.monitor_type = self.site_info.get_monitor_type().to_string();
    }

    pub fn process_file(
        &mut self,
        file_path: &str
    ) -> Result<ProcessedFileData, FileProcessorError> {
        let mut file_data = self.read_file(file_path)?;
        let timestamp_column = self.identify_timestamp_column(&file_data)?;
        self.time_col = Some(timestamp_column.clone());
        let timestamp_format = self.identify_timestamp_format(&file_data, &timestamp_column)?;
        self.parse_dates(&mut file_data, &timestamp_column, &timestamp_format)?;
        let (file_data_with_series, gap_count) = self.create_timestamp_series(
            &file_data,
            &timestamp_column,
            "%Y-%m-%d %H:%M:%S"
        )?;

        let mut series_vec: Vec<Series> = Vec::new();
        for (i, header) in file_data_with_series.headers.iter().enumerate() {
            let series = if header == &timestamp_column {
                let timestamps: Vec<NaiveDateTime> = file_data_with_series.data
                    .iter()
                    .map(|row| NaiveDateTime::parse_from_str(&row[i], "%Y-%m-%d %H:%M:%S").unwrap())
                    .collect();
                Series::new(header.into(), timestamps)
            } else {
                let values: Vec<f64> = file_data_with_series.data
                    .iter()
                    .map(|row| row[i].parse::<f64>().unwrap_or(f64::NAN))
                    .collect();
                Series::new(header.into(), values)
            };
            series_vec.push(series);
        }

        let df = DataFrame::new(series_vec)?;
        self.df = Some(df.clone());

        // Get start and end timestamps
        let (start, end) = self.get_start_end_timestamps(
            &file_data_with_series,
            &timestamp_column,
            "%Y-%m-%d %H:%M:%S"
        )?;

        // Extract column names and indices
        let column_mapping = self.get_column_names_and_indices(file_path)?;

        // Determine monitor type
        self.determine_monitor_type(file_path, &column_mapping);
        self.site_info
            .extract_site_info(file_path, &column_mapping)
            .map_err(|e| FileProcessorError::ParseError(e.to_string()))?;

        let processed_data = ProcessedFileData {
            df,
            start_timestamp: start,
            end_timestamp: end,
            gaps_filled: gap_count,
            interval: self.interval.unwrap(),
            column_mapping,
            monitor_type: self.monitor_type.clone(),
            site_id: self.site_info.get_site_id().into(),
            site_name: self.site_info.get_site_name().into(),
        };

        // Update internal state
        self.df = Some(processed_data.df.clone());
        self.start_timestamp = Some(processed_data.start_timestamp.clone());
        self.end_timestamp = Some(processed_data.end_timestamp.clone());

        Ok(processed_data)
    }

    fn calculate_interval_from_df(
        &self,
        df: &DataFrame,
        time_col: &str
    ) -> Result<Duration, FileProcessorError> {
        let time_series = df.column(time_col)?;
        let mut timestamps: Vec<NaiveDateTime> = time_series
            .datetime()?
            .as_datetime_iter()
            .filter_map(|opt_dt| opt_dt)
            .collect();

        timestamps.sort_unstable();

        let mut intervals = HashMap::new();
        for window in timestamps.windows(2) {
            if let [prev, next] = window {
                let diff = *next - *prev;
                *intervals.entry(diff).or_insert(0) += 1;
            }
        }

        intervals
            .into_iter()
            .max_by_key(|&(_, count)| count)
            .map(|(interval, _)| interval)
            .ok_or_else(|| {
                FileProcessorError::ParseError("Could not determine a mode interval".to_string())
            })
    }

    pub fn update_timestamps(
        &mut self,
        start_time: &str,
        end_time: &str
    ) -> Result<UpdatedTimestampData, FileProcessorError> {
        // Check if DataFrame is loaded
        let df = self.df
            .as_mut()
            .ok_or(
                FileProcessorError::ParseError(
                    "No data loaded. Cannot update timestamps.".to_string()
                )
            )?;

        // Check if time column is identified
        let time_col = self.time_col.as_ref().ok_or(FileProcessorError::TimestampColumnNotFound)?;

        // Parse the new start and end times
        let new_start = NaiveDateTime::parse_from_str(start_time, "%Y-%m-%d %H:%M:%S").map_err(|_| {
            FileProcessorError::ParseError("Failed to parse start timestamp".to_string())
        })?;
        let new_end = NaiveDateTime::parse_from_str(end_time, "%Y-%m-%d %H:%M:%S").map_err(|_| {
            FileProcessorError::ParseError("Failed to parse end timestamp".to_string())
        })?;

        if new_start >= new_end {
            return Err(
                FileProcessorError::ParseError("Start time must be before end time".to_string())
            );
        }

        // Filter the DataFrame based on the new time range
        let mask = df
            .column(time_col)?
            .datetime()?
            .as_datetime_iter()
            .map(|opt_dt| {
                opt_dt
                    .map(|dt| {
                        dt.and_utc().timestamp_nanos_opt() >=
                            new_start.and_utc().timestamp_nanos_opt() &&
                            dt.and_utc().timestamp_nanos_opt() <=
                                new_end.and_utc().timestamp_nanos_opt()
                    })
                    .unwrap_or(false)
            })
            .collect::<BooleanChunked>();

        let filtered_df = df.filter(&mask)?;

        if filtered_df.height() == 0 {
            return Err(
                FileProcessorError::ParseError("No data in the specified time range".to_string())
            );
        }

        // Update start and end timestamps
        self.start_timestamp = Some(start_time.to_string());
        self.end_timestamp = Some(end_time.to_string());

        if self.interval.is_none() {
            self.interval = Some(self.calculate_interval_from_df(&filtered_df, time_col)?);
        }

        self.df = Some(filtered_df);

        Ok(UpdatedTimestampData {
            start_timestamp: start_time.to_string(),
            end_timestamp: end_time.to_string(),
            interval: self.interval.unwrap(),
            row_count: self.df.as_ref().unwrap().height(),
        })
    }
}
