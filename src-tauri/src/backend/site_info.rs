use regex::Regex;
use std::collections::HashMap;
use std::error::Error;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct SiteInfo {
    site_id: String,
    site_name: String,
    monitor_type: String,
}

impl SiteInfo {
    pub fn new() -> Self {
        SiteInfo {
            site_id: String::from("Unknown"),
            site_name: String::from("Unknown"),
            monitor_type: String::from("Unknown"),
        }
    }

    pub fn extract_site_info(
        &mut self,
        filename: &str,
        column_mapping: &HashMap<String, Vec<(String, usize, Option<String>, Option<String>)>>,
    ) -> Result<(), Box<dyn Error>> {
        self.extract_from_filename(filename)?;
        if self.site_id == "Unknown" {
            self.extract_from_column_mapping(column_mapping);
        }
        self.determine_monitor_type(filename, column_mapping);
        self.finalize();
        Ok(())
    }

    pub(crate) fn extract_from_filename(&mut self, filename: &str) -> Result<(), Box<dyn Error>> {
        let name_without_ext = Path::new(filename)
            .file_stem()
            .ok_or("Invalid filename")?
            .to_str()
            .ok_or("Filename contains invalid UTF-8")?;

        let sitename_regex = Regex::new(r"^([A-Za-z]+\d+)$")?;
        let siteid_regex = Regex::new(r"^(\d+)$")?;

        if let Some(captures) = sitename_regex.captures(name_without_ext) {
            self.site_id = captures[1].to_string();
            self.site_name = captures[1].to_string();
        } else if let Some(captures) = siteid_regex.captures(name_without_ext) {
            self.site_id = captures[1].to_string();
        }

        Ok(())
    }

    pub(crate) fn extract_from_column_mapping(
        &mut self,
        column_mapping: &HashMap<String, Vec<(String, usize, Option<String>, Option<String>)>>,
    ) {
        for (_, columns) in column_mapping {
            for (_, _, site_id, _) in columns {
                if let Some(id) = site_id {
                    self.site_id = id.clone();
                    return;
                }
            }
        }
    }
    //todo: need to add logic for the column if the name is Level

    pub(crate) fn determine_monitor_type(
        &mut self,
        filename: &str,
        column_mapping: &HashMap<String, Vec<(String, usize, Option<String>, Option<String>)>>,
    ) {
        let filename_lower = filename.to_lowercase();
        if filename_lower.contains("dm") || filename_lower.contains("depth") {
            self.monitor_type = String::from("Depth");
        } else if filename_lower.contains("fm") || filename_lower.contains("flow") {
            self.monitor_type = String::from("Flow");
        } else if filename_lower.contains("rg") || filename_lower.contains("rain") {
            self.monitor_type = String::from("Rainfall");
        } else {
            self.determine_monitor_type_from_columns(column_mapping);
        }
    }

    fn determine_monitor_type_from_columns(
        &mut self,
        column_mapping: &HashMap<String, Vec<(String, usize, Option<String>, Option<String>)>>,
    ) {
        if column_mapping.contains_key("rainfall") {
            self.monitor_type = String::from("Rainfall");
        } else if column_mapping.contains_key("flow")
            || (column_mapping.contains_key("depth") && column_mapping.contains_key("velocity"))
        {
            self.monitor_type = String::from("Flow");
        } else if column_mapping.contains_key("depth") {
            self.monitor_type = String::from("Depth");
        }
    }

    pub fn finalize(&mut self) {
        if self.site_name == "Unknown" && self.site_id != "Unknown" {
            self.site_name = self.site_id.clone();
        }
    }

    pub fn get_site_id(&self) -> &str {
        &self.site_id
    }

    pub fn get_site_name(&self) -> &str {
        &self.site_name
    }

    pub fn get_monitor_type(&self) -> &str {
        &self.monitor_type
    }
}
