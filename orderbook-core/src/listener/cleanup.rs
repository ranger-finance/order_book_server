use chrono::{Local, Timelike};
use log::{error, info, warn};
use std::path::PathBuf;
use tokio::fs;

const HL_DATA_DIRS: &[&str] = &["node_fills_by_block", "node_order_statuses_by_block", "node_raw_book_diffs_by_block"];

pub async fn perform_cleanup(
    base_dir: PathBuf,
    retention_days: i64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Starting cleanup with retention period of {} days", retention_days);

    let current_date = Local::now();
    info!("Current date: {}", current_date.format("%Y%m%d"));

    for dir_name in HL_DATA_DIRS {
        let dir_path = base_dir.join(dir_name);
        if !dir_path.exists() {
            warn!("Directory {} does not exist, skipping", dir_path.display());
            continue;
        }

        if let Err(err) = cleanup_directory(dir_path, &current_date, retention_days).await {
            error!("Error cleaning up {}: {}", dir_name, err);
        }
    }

    info!("Cleanup completed");
    Ok(())
}

async fn cleanup_directory(
    dir_path: PathBuf,
    current_date: &chrono::DateTime<Local>,
    retention_days: i64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut entries = fs::read_dir(&dir_path).await?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();

        if !path.is_dir() {
            continue;
        }

        let folder_name = path.file_name().and_then(|s| s.to_str()).ok_or("Invalid folder name")?;

        if let Ok(folder_date) = chrono::NaiveDate::parse_from_str(folder_name, "%Y%m%d") {
            let folder_datetime = folder_date.and_hms_opt(0, 0, 0).ok_or("Failed to create datetime")?;
            let folder_chrono =
                chrono::DateTime::<Local>::from_naive_utc_and_offset(folder_datetime, *Local::now().offset());
            let days_old = current_date.signed_duration_since(folder_chrono).num_days();

            if days_old > retention_days {
                info!("Removing old directory: {} ({} days old)", path.display(), days_old);
                if let Err(err) = fs::remove_dir_all(&path).await {
                    error!("Failed to remove directory {}: {}", path.display(), err);
                }
            } else if days_old == 0 {
                info!("Cleaning hourly files in current date directory: {}", path.display());
                if let Err(err) = cleanup_hourly_files(&path, current_date).await {
                    error!("Failed to clean hourly files in {}: {}", path.display(), err);
                }
            }
        }
    }

    Ok(())
}

async fn cleanup_hourly_files(
    dir_path: &PathBuf,
    current_date: &chrono::DateTime<Local>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let current_hour = current_date.hour() as i64;
    let retention_hours = 2;

    let mut entries = fs::read_dir(dir_path).await?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();

        if path.is_dir() {
            continue;
        }

        let file_name = path.file_name().and_then(|s| s.to_str()).ok_or("Invalid file name")?;

        if let Ok(file_time) = file_name.parse::<i64>() {
            if file_time >= 100_000 && file_time <= 23_59_59 {
                let file_hour = file_time / 10_000;

                if current_hour - file_hour > retention_hours {
                    info!("Removing old hourly file: {}", path.display());
                    if let Err(err) = fs::remove_file(&path).await {
                        error!("Failed to remove file {}: {}", path.display(), err);
                    }
                }
            }
        }
    }

    Ok(())
}
