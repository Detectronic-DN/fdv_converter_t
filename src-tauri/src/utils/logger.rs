use chrono::Local;
use log::{Level, LevelFilter, Metadata, Record, SetLoggerError};
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::Mutex;
use tauri::Emitter;

static LOGGER: Mutex<Option<Logger>> = Mutex::new(None);

#[derive(Clone, Serialize)]
pub struct LogMessage {
    level: String,
    message: String,
    timestamp: String,
}

pub struct Logger {
    app_handle: tauri::AppHandle,
    recent_logs: Mutex<VecDeque<LogMessage>>,
    console_logging_enabled: Mutex<bool>,
    frontend_logging_enabled: Mutex<bool>,
}

impl Logger {
    pub fn init(
        app_handle: tauri::AppHandle,
        max_recent_logs: usize,
    ) -> Result<(), SetLoggerError> {
        let logger = Logger {
            app_handle,
            recent_logs: Mutex::new(VecDeque::with_capacity(max_recent_logs)),
            console_logging_enabled: Mutex::new(true),
            frontend_logging_enabled: Mutex::new(true),
        };

        let mut global_logger = LOGGER.lock().unwrap();
        *global_logger = Some(logger);

        log::set_logger(&*Box::leak(Box::new(LoggerImplementation)))?;
        log::set_max_level(LevelFilter::Info);

        Ok(())
    }

    fn log(&self, record: &Record) {
        if should_filter_log(record) {
            return;
        }
        let level = record.level();
        let args = record.args();
        let target = record.target();
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();

        let log_entry = format!("[{}] {} - {}: {}", timestamp, level, target, args);
        let log_message = LogMessage {
            level: level.to_string(),
            message: args.to_string(),
            timestamp: timestamp.clone(),
        };

        // Write to console if enabled
        if *self.console_logging_enabled.lock().unwrap() {
            println!("{}", log_entry);
        }

        // Send to frontend if enabled
        if *self.frontend_logging_enabled.lock().unwrap() {
            self.app_handle
                .emit("log_message", log_message.clone())
                .expect("Failed to emit log message");
        }

        // Add to recent logs
        let mut recent_logs = self.recent_logs.lock().unwrap();
        if recent_logs.len() >= recent_logs.capacity() {
            recent_logs.pop_front();
        }
        recent_logs.push_back(log_message);
    }

    pub fn get_recent_logs(&self) -> Vec<LogMessage> {
        self.recent_logs.lock().unwrap().iter().cloned().collect()
    }

    pub fn set_console_logging(&self, enabled: bool) {
        *self.console_logging_enabled.lock().unwrap() = enabled;
    }

    pub fn set_frontend_logging(&self, enabled: bool) {
        *self.frontend_logging_enabled.lock().unwrap() = enabled;
    }
}

fn should_filter_log(record: &Record) -> bool {
    record
        .target()
        .starts_with("tao::platform_impl::platform::event_loop::runner")
        && (record
            .args()
            .to_string()
            .contains("NewEvents emitted without explicit RedrawEventsCleared")
            || record
                .args()
                .to_string()
                .contains("RedrawEventsCleared emitted without explicit MainEventsCleared"))
}

struct LoggerImplementation;

impl log::Log for LoggerImplementation {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) && !should_filter_log(record) {
            if let Some(logger) = LOGGER.lock().unwrap().as_ref() {
                logger.log(record);
            }
        }
    }

    fn flush(&self) {}
}

#[tauri::command]
pub fn get_recent_logs() -> Vec<LogMessage> {
    LOGGER
        .lock()
        .unwrap()
        .as_ref()
        .map(|logger| logger.get_recent_logs())
        .unwrap_or_default()
}

#[tauri::command]
pub fn set_console_logging(enabled: bool) {
    if let Some(logger) = LOGGER.lock().unwrap().as_ref() {
        logger.set_console_logging(enabled);
    }
}

#[tauri::command]
pub fn set_frontend_logging(enabled: bool) {
    if let Some(logger) = LOGGER.lock().unwrap().as_ref() {
        logger.set_frontend_logging(enabled);
    }
}

#[tauri::command]
pub fn clear_logs() {
    if let Some(logger) = LOGGER.lock().unwrap().as_ref() {
        let mut recent_logs = logger.recent_logs.lock().unwrap();
        recent_logs.clear();
    }
}
