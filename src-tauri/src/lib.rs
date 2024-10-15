mod backend;
mod calculations;
mod fdv;
mod utils;

use log::LevelFilter;
use tauri_plugin_updater::UpdaterExt;
use utils::commands::*;
use utils::logger::{get_recent_logs, set_console_logging, set_frontend_logging, Logger};

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_process::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .manage(create_app_state())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_shell::init())
        .setup(|app| {
            let app_handle = app.handle();
            Logger::init(app_handle.clone(), 100).expect("Failed to initialize logger");
            log::set_max_level(LevelFilter::Info);

            // Spawn the update checker
            let update_handle = app_handle.clone();
            tauri::async_runtime::spawn(async move {
                if let Err(e) = check_update(update_handle).await {
                    log::error!("Failed to check for updates: {}", e);
                }
            });

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

async fn check_update(app: tauri::AppHandle) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(update) = app.updater().unwrap().check().await? {
        let mut downloaded = 0;
        update
            .download_and_install(
                |chunk_length, content_length| {
                    downloaded += chunk_length;
                    log::info!(
                        "Downloaded {} bytes out of {:?} bytes",
                        downloaded,
                        content_length
                    );
                },
                || {
                    log::info!("Download finished");
                },
            )
            .await?;

        log::info!("Update installed successfully");
        app.restart();
    } else {
        log::info!("No updates available");
    }

    Ok(())
}