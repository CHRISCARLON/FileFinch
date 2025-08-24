# FileFinch
<img alt="Crates.io Version" src="https://img.shields.io/crates/v/file_finch"> <img alt="Crates.io Downloads" src="https://img.shields.io/crates/d/file_finch">

Reliable file format detection library written in Rust.

FileFinch can identify various data formats by examining file contents and magic bytes.

## Features

Detects multiple file formats including:

```rust
pub enum FileType {
    Geopackage,
    Shapefile,
    Geojson,
    Excel,
    Csv,
    Parquet,
    Arrow,
    Png,
    Unknown,
}
```
