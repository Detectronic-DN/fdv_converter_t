mod backend;
mod calculations;
mod fdv;
mod utils;

use log::LevelFilter;
use utils::commands::*;
use utils::logger::{get_recent_logs, set_console_logging, set_frontend_logging, Logger};

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_updater::Builder::new().build())
        .manage(create_app_state())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_shell::init())
        .setup(|app| {
            let app_handle = app.handle();
            Logger::init(app_handle.clone(), 100).expect("Failed to initialize logger");
            log::set_max_level(LevelFilter::Info);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            greet,
            process_file,
            update_timestamps,
            clear_command_handler_state,
            get_recent_logs,
            set_console_logging,
            set_frontend_logging,
            update_site_name,
            update_site_id,
            create_fdv_flow,
            create_rainfall,
            calculate_r3,
            run_batch_process,
            generate_interim_reports,
            generate_rainfall_totals
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
