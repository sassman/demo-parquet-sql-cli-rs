use anyhow::Result;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::*;
use dialoguer::theme::ColorfulTheme;
use dialoguer::{History, Input};
use std::collections::VecDeque;
use std::fs::read_dir;
use std::path::PathBuf;
use std::sync::Arc;

struct MyHistory {
    history: VecDeque<String>,
}

impl<T: ToString> History<T> for MyHistory {
    fn read(&self, pos: usize) -> Option<String> {
        self.history.get(pos).cloned()
    }

    fn write(&mut self, val: &T) {
        self.history.push_front(val.to_string());
    }
}

impl Default for MyHistory {
    fn default() -> Self {
        MyHistory {
            history: VecDeque::new(),
        }
    }
}

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();
    let file_format = ParquetFormat::default().with_enable_pruning(true);
    let mut history = MyHistory::default();

    let listing_options = ListingOptions {
        file_extension: "parquet".to_owned(),
        format: Arc::new(file_format),
        table_partition_cols: Default::default(),
        collect_stat: true,
        target_partitions: 1,
        file_sort_order: Default::default(),
    };

    for entry in read_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data"))? {
        register_file_as_table(&ctx, &listing_options, entry?.path()).await?;
    }

    loop {
        println!();
        if let Ok(sql) = Input::<String>::with_theme(&ColorfulTheme::default())
            .with_prompt("sql ❯")
            .history_with(&mut history)
            .interact_text()
        {
            // execute the query
            let res = ctx.sql(&sql).await;
            if res.is_ok() {
                // print the results
                res?.show().await?;
            } else {
                eprintln!("{:?}", res);
            }
        } else {
            break;
        }
    }

    Ok(())
}

async fn register_file_as_table(
    ctx: &SessionContext,
    listing_options: &ListingOptions,
    path: PathBuf,
) -> Result<Option<()>> {
    let extension = path.extension().unwrap().to_str().unwrap();
    if path.is_file() && extension.eq(&listing_options.file_extension) {
        let name = path.file_stem().unwrap().to_str().unwrap().to_string();
        let file_path = path
            .as_path()
            .strip_prefix(PathBuf::from(env!("CARGO_MANIFEST_DIR")))?
            .to_str()
            .unwrap()
            .to_string();

        println!("- register file {file_path} as table `{name}`");

        ctx.register_listing_table(&name, file_path, listing_options.clone(), None, None)
            .await?;
    }

    Ok(Some(()))
}
