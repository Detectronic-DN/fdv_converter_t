use crate::backend::backend::CommandHandler;
use rayon::prelude::*;
use serde_json::Value;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use zip::write::{FileOptions, ZipWriter};
use zip::CompressionMethod;

#[derive(Debug, Clone)]
pub struct ProcessedFileInfo {
    pub conversion_output_path: Option<PathBuf>,
}

#[derive(Debug, thiserror::Error)]
pub enum BatchProcessingError {
    #[error("File processing error: {0}")]
    FileProcessingError(String),
    #[error("JSON parsing error: {0}")]
    JsonParsingError(#[from] serde_json::Error),
    #[error("Lock error: {0}")]
    LockError(String),
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
}

pub struct BatchProcessor {
    command_handler: Arc<Mutex<CommandHandler>>,
    pub processed_files: Vec<ProcessedFileInfo>,
}

impl BatchProcessor {
    pub fn new() -> Self {
        BatchProcessor {
            command_handler: Arc::new(Mutex::new(CommandHandler::new())),
            processed_files: Vec::new(),
        }
    }

    pub fn process_convert_and_zip(
        &mut self,
        file_infos: Vec<Value>,
        output_dir: &Path,
    ) -> Result<PathBuf, BatchProcessingError> {
        log::info!("Starting file processing and conversion...");

        fs::create_dir_all(output_dir)?;

        let results: Result<Vec<_>, _> = file_infos
            .into_par_iter()
            .map(|file_info| {
                let input_path =
                    PathBuf::from(file_info["filepath"].as_str().ok_or_else(|| {
                        BatchProcessingError::FileProcessingError("Invalid filepath".to_string())
                    })?);

                log::info!("Processing file: {:?}", input_path);

                if !input_path.exists() {
                    return Err(BatchProcessingError::FileProcessingError(format!(
                        "Input file does not exist: {:?}",
                        input_path
                    )));
                }

                let output_path =
                    self.process_and_convert_file(&file_info, &input_path, output_dir)?;

                let processed_file_info = ProcessedFileInfo {
                    conversion_output_path: Some(output_path),
                };
                Ok(processed_file_info)
            })
            .collect();

        self.processed_files = results?;

        log::info!("File processing and conversion completed. Starting zip creation...");

        let zip_path = output_dir.join("processed_files.zip");
        self.create_zip_file(&zip_path)?;

        log::info!("Zip file created successfully at: {:?}", zip_path);

        Ok(zip_path)
    }

    fn process_and_convert_file(
        &self,
        file_info: &Value,
        input_path: &Path,
        output_dir: &Path,
    ) -> Result<PathBuf, BatchProcessingError> {
        let mut ch = self
            .command_handler
            .lock()
            .map_err(|e| BatchProcessingError::LockError(e.to_string()))?;

        let process_result: Value = ch
            .process_file(input_path.to_str().unwrap())
            .map_err(|e| {
                BatchProcessingError::FileProcessingError(format!("Failed to process file: {}", e))
            })
            .and_then(|json_str| {
                serde_json::from_str(&json_str).map_err(BatchProcessingError::JsonParsingError)
            })?;

        if !process_result["success"].as_bool().unwrap_or(false) {
            return Err(BatchProcessingError::FileProcessingError(
                "File processing failed".to_string(),
            ));
        }

        let monitor_type = process_result["monitorType"].as_str().ok_or_else(|| {
            BatchProcessingError::FileProcessingError("Invalid monitor type".to_string())
        })?;
        let column_mapping = process_result["columnMapping"].as_object().ok_or_else(|| {
            BatchProcessingError::FileProcessingError("Invalid column mapping".to_string())
        })?;
        let site_name = process_result["siteName"].as_str().ok_or_else(|| {
            BatchProcessingError::FileProcessingError("Site name not found".to_string())
        })?;

        let file_extension = if monitor_type == "rainfall" {
            "r"
        } else {
            "fdv"
        };
        let output_filename = format!("{}.{}", site_name, file_extension);
        let output_path = output_dir.join(output_filename);

        match monitor_type {
            "Flow" | "Depth" => {
                let pipe_shape = file_info["pipeshape"].as_str().ok_or_else(|| {
                    BatchProcessingError::FileProcessingError(
                        "Pipe shape is required for flow/depth conversion".to_string(),
                    )
                })?;
                let pipe_size = file_info["pipesize"].as_str().ok_or_else(|| {
                    BatchProcessingError::FileProcessingError(
                        "Pipe size is required for flow/depth conversion".to_string(),
                    )
                })?;

                ch.create_fdv_flow(
                    output_path.to_str().unwrap(),
                    &Self::extract_column_name(column_mapping, "depth")?,
                    &Self::extract_column_name(column_mapping, "velocity")?,
                    pipe_shape,
                    pipe_size,
                )
            }
            "Rainfall" => ch.create_rainfall(
                output_path.to_str().unwrap(),
                &Self::extract_column_name(column_mapping, "rainfall")?,
            ),
            _ => Err(format!("Unsupported monitor type: {}", monitor_type)),
        }
        .map_err(|e| {
            BatchProcessingError::FileProcessingError(format!(
                "Failed to create output file: {}",
                e
            ))
        })?;

        Ok(output_path)
    }

    fn create_zip_file(&self, zip_path: &Path) -> Result<(), BatchProcessingError> {
        let file = File::create(zip_path).map_err(|e| {
            BatchProcessingError::FileProcessingError(format!("Failed to create zip file: {}", e))
        })?;
        let mut zip = ZipWriter::new(file);
        for processed_file in &self.processed_files {
            if let Some(output_path) = &processed_file.conversion_output_path {
                log::info!("Adding file to zip: {:?}", output_path);
                if !output_path.exists() {
                    return Err(BatchProcessingError::FileProcessingError(format!(
                        "Processed file does not exist: {:?}",
                        output_path
                    )));
                }
                let options: FileOptions<'static, ()> = FileOptions::default()
                    .compression_method(CompressionMethod::Deflated)
                    .unix_permissions(0o755);
                let file_name = output_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| {
                        BatchProcessingError::FileProcessingError(
                            "Invalid or non-UTF8 file name".to_string(),
                        )
                    })?;
                zip.start_file(file_name, options).map_err(|e| {
                    BatchProcessingError::FileProcessingError(format!(
                        "Failed to start file in zip: {}",
                        e
                    ))
                })?;
                let mut file = File::open(output_path).map_err(|e| {
                    BatchProcessingError::FileProcessingError(format!(
                        "Failed to open processed file: {}",
                        e
                    ))
                })?;
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer).map_err(|e| {
                    BatchProcessingError::FileProcessingError(format!(
                        "Failed to read processed file: {}",
                        e
                    ))
                })?;
                zip.write_all(&buffer).map_err(|e| {
                    BatchProcessingError::FileProcessingError(format!(
                        "Failed to write to zip: {}",
                        e
                    ))
                })?;
            }
        }
        zip.finish().map_err(|e| {
            BatchProcessingError::FileProcessingError(format!("Failed to finish zip file: {}", e))
        })?;
        Ok(())
    }

    fn extract_column_name(
        column_mapping: &serde_json::Map<String, Value>,
        key: &str,
    ) -> Result<String, BatchProcessingError> {
        column_mapping
            .get(key)
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|v| v.as_str())
            .map(String::from)
            .ok_or_else(|| {
                BatchProcessingError::FileProcessingError(format!(
                    "Failed to extract column name for key: {}",
                    key
                ))
            })
    }
}
