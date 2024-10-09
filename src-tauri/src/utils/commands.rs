use std::sync::Mutex;
use tauri::State;
use crate::backend::backend::CommandHandler;

pub struct AppState {
    command_handler: Mutex<CommandHandler>,
}

#[tauri::command]
pub fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

#[tauri::command]
pub async fn process_file(state: State<'_, AppState>, file_path: String) -> Result<String, String> {
    let mut command_handler = state.command_handler.lock().map_err(|_| "Failed to acquire lock on CommandHandler".to_string())?;
    command_handler.process_file(&file_path)
}

#[tauri::command]
pub async fn update_timestamps(state: State<'_, AppState>, start_time: String, end_time: String) -> Result<String, String> {
    let mut command_handler = state.command_handler.lock().map_err(|_| "Failed to acquire lock on CommandHandler".to_string())?;
    command_handler.update_timestamps(&start_time, &end_time)
}

#[tauri::command]
pub fn clear_command_handler_state(state: State<'_, AppState>) -> Result<(), String> {
    let mut command_handler = state.command_handler.lock().map_err(|_| "Failed to acquire lock on CommandHandler".to_string())?;
    command_handler.reset();

    Ok(())
}

#[tauri::command]
pub async fn update_site_id(state: State<'_, AppState>, site_id: String) -> Result<String, String> {
    let mut command_handler = state.command_handler.lock().map_err(|_| "Failed to acquire lock on CommandHandler".to_string())?;
    command_handler.update_site_id(site_id)
}

#[tauri::command]
pub async fn update_site_name(state: State<'_, AppState>, site_name: String) -> Result<String, String> {
    let mut command_handler = state.command_handler.lock().map_err(|_| "Failed to acquire lock on CommandHandler".to_string())?;
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
    velocity_col: String,
    pipe_shape: String,
    pipe_size: String,
) -> Result<String, String> {
    let mut command_handler = state.command_handler.lock().map_err(|_| "Failed to acquire lock on CommandHandler".to_string())?;

    // Call the create_fdv_flow method and return its result
    command_handler.create_fdv_flow(&output_path, &depth_col, &velocity_col, &pipe_shape, &pipe_size)
}

#[tauri::command]
pub fn create_rainfall(
    state: State<'_, AppState>,
    output_path: String,
    rainfall_col: String,
) -> Result<String, String> {
    let mut command_handler = state.command_handler.lock().map_err(|_| "Failed to acquire lock on CommandHandler".to_string())?;

    command_handler.create_rainfall(&output_path, &rainfall_col)
}