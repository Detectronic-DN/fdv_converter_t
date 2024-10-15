use crate::backend::backend::CommandHandler;
use chrono::{Duration, NaiveDateTime};
use polars::prelude::*;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum InterimReportError {
    ColumnExtractionError(String),
    DataFrameError(String),
    InvalidMonitorType(String),
}

impl fmt::Display for InterimReportError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            InterimReportError::ColumnExtractionError(msg) => write!(f, "Column extraction error: {}", msg),
            InterimReportError::DataFrameError(msg) => write!(f, "DataFrame error: {}", msg),
            InterimReportError::InvalidMonitorType(msg) => write!(f, "Invalid monitor type: {}", msg),
        }
    }
}

impl Error for InterimReportError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MonitorType {
    Flow,
    Depth,
    Rainfall,
}

impl MonitorType {
    fn from_str(s: &str) -> Result<Self, InterimReportError> {
        match s.to_lowercase().as_str() {
            "flow" => Ok(MonitorType::Flow),
            "depth" => Ok(MonitorType::Depth),
            "rainfall" => Ok(MonitorType::Rainfall),
            _ => Err(InterimReportError::InvalidMonitorType(format!("'{}' is not a valid monitor type", s))),
        }
    }
}

pub struct InterimReportGenerator {
    monitor_type: MonitorType,
    df: DataFrame,
    interval: Duration,
    time_column: String,
    flow_column: String,
    depth_column: String,
    rainfall_column: String,
}

impl<'a> InterimReportGenerator {
    pub fn new(backend: &'a CommandHandler) -> Result<Self, InterimReportError> {
        let columns = backend.column_mapping.clone();
        let monitor_type = MonitorType::from_str(&backend.monitor_type)?;
        let df = backend
            .data_frame
            .as_ref()
            .ok_or_else(|| {
                InterimReportError::DataFrameError("No data frame available".to_string())
            })?
            .clone();
        let interval = backend.interval;
        let time_col = backend.time_col.clone().unwrap().to_string();

        let extract_column_name = |name: &str| -> Result<Option<String>, InterimReportError> {
            let column_mapping = serde_json::to_value(columns.clone())
                .map_err(|e| InterimReportError::ColumnExtractionError(e.to_string()))?;

            column_mapping
                .get(name)
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|v| v.as_str())
                .map(String::from)
                .ok_or_else(|| {
                    InterimReportError::ColumnExtractionError(format!(
                        "Failed to extract column name for key: {}",
                        name
                    ))
                })
                .map(Some)
        };

        let flow_column = match monitor_type {
            MonitorType::Flow => extract_column_name("flow")?,
            _ => None,
        };

        let depth_column = match monitor_type {
            MonitorType::Depth | MonitorType::Flow => extract_column_name("depth")?,
            _ => None,
        };

        let rainfall_column = match monitor_type {
            MonitorType::Rainfall => extract_column_name("rainfall")?,
            _ => None,
        };

        Ok(Self {
            monitor_type,
            df,
            interval,
            time_column: time_col,
            flow_column: flow_column.unwrap_or_default(),
            depth_column: depth_column.unwrap_or_default(),
            rainfall_column: rainfall_column.unwrap_or_default(),
        })
    }

    fn calculate_values(&mut self) -> Result<&DataFrame, Box<dyn Error>> {
        match self.monitor_type {
            MonitorType::Flow => {
                let interval_seconds = self.interval.num_seconds();
                let liters_expr =
                    col(&self.flow_column).cast(DataType::Float64) * lit(interval_seconds);
                let m3_expr = liters_expr.clone() / lit(1000.0);

                self.df = self
                    .df
                    .clone()
                    .lazy()
                    .with_column(liters_expr.alias("L"))
                    .with_column(m3_expr.alias("m3"))
                    .collect()?;
            }
            _ => println!(
                "No calculation needed for monitor type: {:?}",
                self.monitor_type
            ),
        }
        Ok(&self.df)
    }

    fn generate_weekly_summary(
        &self,
        weekly_data: &DataFrame,
    ) -> Result<HashMap<String, String>, Box<dyn Error>> {
        let mut summary = HashMap::new();

        match self.monitor_type {
            MonitorType::Flow => {
                let total_flow: f64 = weekly_data.column("m3")?.sum()?;
                let max_flow: f64 = weekly_data.column(&self.flow_column)?.max()?.unwrap();
                let min_flow: f64 = weekly_data.column(&self.flow_column)?.min()?.unwrap();

                summary.insert("Total Flow(m3)".to_string(), total_flow.to_string());
                summary.insert("Max Flow(l/s)".to_string(), max_flow.to_string());
                summary.insert("Min Flow(l/s)".to_string(), min_flow.to_string());
            }
            MonitorType::Depth => {
                let avg_level: f64 = weekly_data.column(&self.depth_column)?.mean().unwrap();
                let max_level: f64 = weekly_data.column(&self.depth_column)?.max()?.unwrap();
                let min_level: f64 = weekly_data.column(&self.depth_column)?.min()?.unwrap();

                summary.insert("Average Level(m)".to_string(), avg_level.to_string());
                summary.insert("Max Level(m)".to_string(), max_level.to_string());
                summary.insert("Min Level(m)".to_string(), min_level.to_string());
            }
            MonitorType::Rainfall => {
                let total_rainfall: f64 = weekly_data.column(&self.rainfall_column)?.sum()?;
                let max_rainfall: f64 = weekly_data.column(&self.rainfall_column)?.max()?.unwrap();
                let min_rainfall: f64 = weekly_data.column(&self.rainfall_column)?.min()?.unwrap();

                summary.insert("Total Rainfall(mm)".to_string(), total_rainfall.to_string());
                summary.insert("Max Rainfall(mm)".to_string(), max_rainfall.to_string());
                summary.insert("Min Rainfall(mm)".to_string(), min_rainfall.to_string());
            }
        }

        Ok(summary)
    }

    fn generate_summaries(
        &self,
        start_date: Option<String>,
        end_date: Option<String>,
    ) -> Result<DataFrame, Box<dyn Error>> {
        let time_column = &self.time_column;
        let sorted_df = self
            .df
            .clone()
            .lazy()
            .with_column(col(time_column).sort(SortOptions::default()))
            .collect()?;
        let df_time_col = sorted_df.column(time_column)?;

        let start_date = self.get_start_date(start_date, df_time_col)?;
        let end_date = self.get_end_date(end_date, df_time_col)?;

        let mut weekly_summaries: Vec<HashMap<String, String>> = Vec::new();
        let mut current_date = start_date;

        while current_date <= end_date {
            let week_end = (current_date.date() + Duration::days(6))
                .and_hms_opt(23, 59, 59)
                .unwrap();

            let weekly_data = sorted_df
                .clone()
                .lazy()
                .filter(
                    col(time_column)
                        .gt_eq(lit(current_date))
                        .and(col(time_column).lt(lit(week_end))),
                )
                .collect()?;

            if !weekly_data.is_empty() {
                let mut summary = self.generate_weekly_summary(&weekly_data)?;
                summary.insert(
                    "Start Date".to_string(),
                    current_date.date().format("%Y-%m-%d").to_string(),
                );
                summary.insert(
                    "End Date".to_string(),
                    week_end.date().format("%Y-%m-%d").to_string(),
                );
                weekly_summaries.push(summary);
            }

            current_date = week_end + Duration::seconds(1);
        }

        self.create_summary_dataframe(weekly_summaries)
    }

    fn get_start_date(
        &self,
        start_date: Option<String>,
        df_time_col: &Series,
    ) -> Result<NaiveDateTime, Box<dyn Error>> {
        Ok(if let Some(start) = start_date {
            NaiveDateTime::parse_from_str(&start, "%Y-%m-%d")?
                .date()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        } else {
            let min_date = df_time_col
                .datetime()?
                .as_datetime_iter()
                .filter_map(|opt_dt| opt_dt.map(|dt| dt.date()))
                .min()
                .ok_or("No valid dates found in the DataFrame")?;
            min_date.and_hms_opt(0, 0, 0).unwrap()
        })
    }

    fn get_end_date(
        &self,
        end_date: Option<String>,
        df_time_col: &Series,
    ) -> Result<NaiveDateTime, Box<dyn Error>> {
        Ok(if let Some(end) = end_date {
            NaiveDateTime::parse_from_str(&end, "%Y-%m-%d")?
                .date()
                .and_hms_opt(23, 59, 59)
                .unwrap()
        } else {
            let max_date = df_time_col
                .datetime()?
                .as_datetime_iter()
                .filter_map(|opt_dt| opt_dt.map(|dt| dt.date()))
                .max()
                .ok_or("No valid dates found in the DataFrame")?;
            max_date.and_hms_opt(23, 59, 59).unwrap()
        })
    }

    fn create_summary_dataframe(
        &self,
        weekly_summaries: Vec<HashMap<String, String>>,
    ) -> Result<DataFrame, Box<dyn Error>> {
        let mut series_vec: Vec<Series> = Vec::new();

        if let Some(first_summary) = weekly_summaries.first() {
            for key in first_summary.keys() {
                let values: Vec<String> = weekly_summaries
                    .iter()
                    .map(|summary| summary.get(key).cloned().unwrap_or_default())
                    .collect();
                series_vec.push(Series::new(key.into(), values));
            }
        }

        let mut summary_df = DataFrame::new(series_vec)?;
        let n_rows = summary_df.height();
        let interim_period: Vec<String> =
            (0..n_rows).map(|x| format!("Interim {}", x + 1)).collect();
        let interim_series = Series::new("Interim Period".into(), interim_period);
        summary_df.with_column(interim_series)?;

        summary_df = summary_df
            .lazy()
            .with_column(
                (col("Start Date").cast(DataType::String)
                    + lit(" - ")
                    + col("End Date").cast(DataType::String))
                .alias("Date Range"),
            )
            .collect()?;

        let columns = match self.monitor_type {
            MonitorType::Flow => vec![
                "Interim Period",
                "Date Range",
                "Total Flow(m3)",
                "Max Flow(l/s)",
                "Min Flow(l/s)",
            ],
            MonitorType::Depth => vec![
                "Interim Period",
                "Date Range",
                "Average Level(m)",
                "Max Level(m)",
                "Min Level(m)",
            ],
            MonitorType::Rainfall => vec![
                "Interim Period",
                "Date Range",
                "Total Rainfall(mm)",
                "Max Rainfall(mm)",
                "Min Rainfall(mm)",
            ],
        };

        let numeric_columns = match self.monitor_type {
            MonitorType::Flow => vec!["Total Flow(m3)", "Max Flow(l/s)", "Min Flow(l/s)"],
            MonitorType::Depth => vec!["Average Level(m)", "Max Level(m)", "Min Level(m)"],
            MonitorType::Rainfall => {
                vec!["Total Rainfall(mm)", "Max Rainfall(mm)", "Min Rainfall(mm)"]
            }
        };

        let final_df = summary_df
            .select(columns)?
            .lazy()
            .with_columns(
                numeric_columns
                    .into_iter()
                    .map(|col_name| col(col_name).cast(DataType::Float64))
                    .collect::<Vec<_>>(),
            )
            .collect()?;

        Ok(final_df)
    }

    pub fn calculate_daily_summary(&self) -> Result<DataFrame, Box<dyn Error>> {
        let time_column = &self.time_column;

        let daily_summary = match self.monitor_type {
            MonitorType::Flow => self.calculate_flow_summary(time_column)?,
            MonitorType::Depth => self.calculate_depth_summary(time_column)?,
            MonitorType::Rainfall => self.calculate_rainfall_summary(time_column)?,
        };

        let formatted_daily_summary = daily_summary
            .lazy()
            .with_column(col("Date").dt().strftime("%d/%m/%Y"))
            .collect()?;

        Ok(formatted_daily_summary)
    }

    fn calculate_flow_summary(&self, time_column: &str) -> Result<DataFrame, Box<dyn Error>> {
        let flow_column = &self.flow_column;
        self.df
            .clone()
            .lazy()
            .with_column(col(time_column).dt().date().alias("Date"))
            .group_by([col("Date")])
            .agg([
                col(flow_column).mean().alias("Average Flow(l/s)"),
                col(flow_column).max().alias("Max Flow(l/s)"),
                col(flow_column).min().alias("Min Flow(l/s)"),
                col("m3").sum().alias("Flow (m3)"),
            ])
            .sort(
                ["Date"],
                SortMultipleOptions::new().with_order_descending(false),
            )
            .collect()
            .map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    fn calculate_depth_summary(&self, time_column: &str) -> Result<DataFrame, Box<dyn Error>> {
        let depth_column = &self.depth_column;
        self.df
            .clone()
            .lazy()
            .with_column(col(time_column).dt().date().alias("Date"))
            .group_by([col("Date")])
            .agg([
                col(depth_column).mean().alias("Average Level(m)"),
                col(depth_column).max().alias("Max Level(m)"),
                col(depth_column).min().alias("Min Level(m)"),
            ])
            .sort(
                ["Date"],
                SortMultipleOptions::new().with_order_descending(false),
            )
            .collect()
            .map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    fn calculate_rainfall_summary(&self, time_column: &str) -> Result<DataFrame, Box<dyn Error>> {
        let rainfall_column = &self.rainfall_column;
        self.df
            .clone()
            .lazy()
            .with_column(col(time_column).dt().date().alias("Date"))
            .group_by([col("Date")])
            .agg([
                col(rainfall_column).sum().alias("Total Rainfall(mm)"),
                col(rainfall_column).max().alias("Max Rainfall(mm)"),
                col(rainfall_column).min().alias("Min Rainfall(mm)"),
            ])
            .sort(
                ["Date"],
                SortMultipleOptions::new().with_order_descending(false),
            )
            .collect()
            .map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    pub fn generate_report(&mut self) -> Result<(DataFrame, DataFrame, DataFrame), Box<dyn Error>> {
        self.calculate_values()?;
        let summaries_df = self.generate_summaries(None, None)?;
        let daily_summary = self.calculate_daily_summary()?;

        let grand_total_row = self.calculate_grand_total(&summaries_df)?;

        let summaries_with_total =
            self.add_grand_total_to_summaries(summaries_df, grand_total_row)?;

        Ok((summaries_with_total, self.df.clone(), daily_summary))
    }

    fn calculate_grand_total(&self, summaries_df: &DataFrame) -> Result<DataFrame, Box<dyn Error>> {
        let mut grand_total_series = vec![
            Series::new("Interim Period".into(), &["Grand Total"]),
            Series::new("Date Range".into(), &[""]),
        ];

        match self.monitor_type {
            MonitorType::Flow => {
                grand_total_series.push(Series::new(
                    "Total Flow(m3)".into(),
                    &[summaries_df.column("Total Flow(m3)")?.sum::<f64>()?],
                ));
                grand_total_series.push(Series::new(
                    "Max Flow(l/s)".into(),
                    &[summaries_df.column("Max Flow(l/s)")?.max::<f64>()?],
                ));
                grand_total_series.push(Series::new(
                    "Min Flow(l/s)".into(),
                    &[summaries_df.column("Min Flow(l/s)")?.min::<f64>()?],
                ));
            }
            MonitorType::Depth => {
                grand_total_series.push(Series::new(
                    "Average Level(m)".into(),
                    &[summaries_df.column("Average Level(m)")?.mean()],
                ));
                grand_total_series.push(Series::new(
                    "Max Level(m)".into(),
                    &[summaries_df.column("Max Level(m)")?.max::<f64>()?],
                ));
                grand_total_series.push(Series::new(
                    "Min Level(m)".into(),
                    &[summaries_df.column("Min Level(m)")?.min::<f64>()?],
                ));
            }
            MonitorType::Rainfall => {
                grand_total_series.push(Series::new(
                    "Total Rainfall(mm)".into(),
                    &[summaries_df.column("Total Rainfall(mm)")?.sum::<f64>()?],
                ));
                grand_total_series.push(Series::new(
                    "Max Rainfall(mm)".into(),
                    &[summaries_df.column("Max Rainfall(mm)")?.max::<f64>()?],
                ));
                grand_total_series.push(Series::new(
                    "Min Rainfall(mm)".into(),
                    &[summaries_df.column("Min Rainfall(mm)")?.min::<f64>()?],
                ));
            }
        }

        DataFrame::new(grand_total_series).map_err(|e| Box::new(e) as Box<dyn Error>)
    }

    fn add_grand_total_to_summaries(
        &self,
        summaries_df: DataFrame,
        grand_total_row: DataFrame,
    ) -> Result<DataFrame, Box<dyn Error>> {
        let summaries_with_total = summaries_df.vstack(&grand_total_row)?;
        Ok(summaries_with_total)
    }

    pub fn generate_rainfall_totals(&self) -> Result<(DataFrame, DataFrame), Box<dyn Error>> {
        if self.monitor_type != MonitorType::Rainfall {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "This method is only applicable for Rainfall monitor type",
            )));
        }

        let time_col = &self.time_column;
        let rainfall_col = &self.rainfall_column;

        // Calculate the number of readings per hour based on the interval
        let interval_seconds = self.interval.num_seconds();
        let readings_per_hour = if interval_seconds > 0 {
            3600 / interval_seconds
        } else {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Interval is invalid or too large, causing division by zero",
            )));
        };

        // Daily totals
        let daily_totals = self
            .df
            .clone()
            .lazy()
            .group_by([col(time_col).dt().date().alias("Date")])
            .agg([
                (col(rainfall_col).sum().fill_null(0.0) / lit(readings_per_hour))
                    .alias("Daily Total (mm)"),
            ])
            .with_column(col("Daily Total (mm)"))
            .sort(
                ["Date"],
                SortMultipleOptions::new().with_order_descending(false),
            )
            .collect()?;

        // Weekly totals
        let weekly_totals = daily_totals
            .clone()
            .lazy()
            .with_column(col("Date").dt().weekday().alias("Weekday"))
            .with_column(col("Date").dt().year().alias("Year"))
            .with_column(col("Date").dt().week().alias("Week"))
            .group_by([col("Year"), col("Week")])
            .agg([
                col("Daily Total (mm)").sum().alias("Weekly Total (mm)"),
                col("Date").min().alias("Week Starting"),
            ])
            .with_column(col("Week Starting").cast(DataType::Date))
            .select([col("Week Starting"), col("Weekly Total (mm)")])
            .sort(
                ["Week Starting"],
                SortMultipleOptions::new().with_order_descending(false),
            )
            .collect()?;

        Ok((daily_totals, weekly_totals))
        //todo: need to fix first and last columns
    }
}
