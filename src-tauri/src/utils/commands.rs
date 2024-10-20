use crate::backend::backend::CommandHandler;
use serde_json::Value;
use std::path::Path;
use std::sync::Mutex;
use tauri::State;

pub struct AppState {
    command_handler: Mutex<CommandHandler>,
}

#[tauri::command]
pub fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

#[tauri::command]
pub async fn process_file(state: State<'_, AppState>, file_path: String) -> Result<String, String> {
    let mut command_handler = state.command_handler
        .lock()
        .map_err(|_| "Failed to acquire lock on CommandHandler".to_string())?;
    command_handler.process_file(&file_path)
}

#[tauri::command]
pub async fn update_timestamps(
    state: State<'_, AppState>,
    start_time: String,
    end_time: String
) -> Result<String, String> {
    let mut command_handler = state.command_handler
        .lock()
        .map_err(|_| "Failed to acquire lock on CommandHandler".to_string())?;
    command_handler.update_timestamps(&start_time, &end_time)
}

#[tauri::command]
pub fn clear_command_handler_state(state: State<'_, AppState>) -> Result<(), String> {
    let mut command_handler = state.command_handler
        .lock()
        .map_err(|_| "Failed to acquire lock on CommandHandler".to_string())?;
    command_handler.reset();

    Ok(())
}

#[tauri::command]
pub async fn update_site_id(state: State<'_, AppState>, site_id: String) -> Result<String, String> {
    let mut command_handler = state.command_handler
        .lock()
        .map_err(|_| "Failed to acquire lock on CommandHandler".to_string())?;
    command_handler.update_site_id(site_id)
}

#[tauri::command]
pub async fn update_site_name(
    state: State<'_, AppState>,
    site_name: String
) -> Result<String, String> {
    let mut command_handler = state.command_handler
        .lock()
        .map_err(|_| "Failed to acquire lock on CommandHandler".to_string())?;
    command_handler.update_site_name(site_name)
}

pub fn create_app_state() -> AppState {
    AppState {
        command_handler: Mutex::new(CommandHandler::new()),
    }
}

#[tauri::command]
pub fn create_fdv_flow(
    state: State<'_, AppState>,
    output_path: String,
    depth_col: String,
    velocity_col: Option<String>,
    pipe_shape: String,
    pipe_size: String
) -> Result<String, String> {
    let mut command_handler = state.command_handler
        .lock()
        .map_err(|_| "Failed to acquire lock on CommandHandler".to_string())?;

    // Call the create_fdv_flow method and return its result
    command_handler.create_fdv_flow(
        &output_path,
        &depth_col,
        &velocity_col.as_deref(),
        &pipe_shape,
        &pipe_size
    )
}

#[tauri::command]
pub fn create_rainfall(
    state: State<'_, AppState>,
    output_path: String,
    rainfall_col: String
) -> Result<String, String> {
    let mut command_handler = state.command_handler
        .lock()
        .map_err(|_| "Failed to acquire lock on CommandHandler".to_string())?;

    command_handler.create_rainfall(&output_path, &rainfall_col)
}

#[tauri::command]
pub fn calculate_r3(
    state: State<'_, AppState>,
    width: f64,
    height: f64,
    egg_form: String
) -> Result<String, String> {
    let command_handler = state.command_handler
        .lock()
        .map_err(|_| "Failed to acquire lock on CommandHandler".to_string())?;

    let r3_value = command_handler.calculate_r3(width, height, &egg_form);

    if r3_value == -1.0 {
        Err("Failed to calculate R3 value".to_string())
    } else {
        Ok(r3_value.to_string())
    }
}

#[tauri::command]
pub async fn run_batch_process(
    state: State<'_, AppState>,
    file_infos: Vec<Value>,
    output_dir: String
) -> Result<String, String> {
    let command_handler = state.command_handler
        .lock()
        .map_err(|_| "Failed to acquire lock on CommandHandler".to_string())?;
    let output_path = Path::new(&output_dir);

    match command_handler.run_batch_process(file_infos, output_path) {
        Ok(()) => Ok("Batch processing completed successfully".to_string()),
        Err(e) => Err(format!("Error during batch processing: {}", e)),
    }
}

#[tauri::command]
pub async fn generate_interim_reports(
    state: State<'_, AppState>,
    output_path: String
) -> Result<String, String> {
    let command_handler = state.command_handler
        .lock()
        .map_err(|_| "Failed to acquire lock on CommandHandler".to_string())?;

    match command_handler.save_interim_reports_to_excel(&output_path) {
        Ok(()) => Ok(format!("Interim reports saved successfully to {}", output_path)),
        Err(e) => Err(format!("Error generating interim reports: {}", e)),
    }
}

#[tauri::command]
pub async fn generate_rainfall_totals(
    state: State<'_, AppState>,
    output_path: String
) -> Result<String, String> {
    let command_handler = state.command_handler
        .lock()
        .map_err(|_| "Failed to acquire lock on CommandHandler".to_string())?;

    match command_handler.save_rainfall_totals_to_excel(&output_path) {
        Ok(()) => Ok(format!("Rainfall totals saved successfully to {}", output_path)),
        Err(e) => Err(format!("Error generating rainfall totals: {}", e)),
    }
}
