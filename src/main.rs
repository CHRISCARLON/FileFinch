use file_finch::FileFinch;
use muy_zipido::{
    MuyZipido,
    progress_bar::{Colour, Style},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = "https://data.london.gov.uk/download/9ca66bba-b18c-4d2b-8025-a5fe7d0d66e0/6defa131-f57e-4f86-921d-8d023c98155d/LAEI2019-nox-pm-cold-start-grid-emissions.zip";
    println!("Fetching and processing ZIP from: {}", url);

    let extractor = MuyZipido::new(url, 10240)?.with_progress(Style::Blocks, Colour::Magenta);

    let mut total_entries = 0;
    let mut total_bytes = 0;
    let mut file_type_counts = std::collections::HashMap::new();

    for entry_result in extractor {
        match entry_result {
            Ok(entry) => {
                total_entries += 1;
                total_bytes += entry.data.len();

                let detected_type = FileFinch::detect(&entry.data);

                *file_type_counts.entry(detected_type).or_insert(0) += 1;

                println!(
                    "Entry {}: {} ({} bytes) - Type: {}",
                    total_entries,
                    entry.filename,
                    entry.data.len(),
                    detected_type
                );
            }
            Err(e) => {
                eprintln!("Error processing entry: {}", e);
                break;
            }
        }
    }

    println!("\n=== Summary ===");
    println!("Total entries: {}", total_entries);
    println!("Total bytes processed: {}", total_bytes);

    println!("\n=== File Type Distribution ===");
    for (file_type, count) in &file_type_counts {
        println!("{}: {}", file_type, count);
    }

    Ok(())
}
