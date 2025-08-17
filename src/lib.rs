use std::fmt;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum FileType {
    Geopackage,
    Shapefile,
    Geojson,
    Excel,
    Csv,
    Parquet,
    Arrow,
    Unknown,
}

impl fmt::Display for FileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            FileType::Geopackage => "Geopackage",
            FileType::Shapefile => "Shapefile",
            FileType::Geojson => "GeoJSON",
            FileType::Excel => "Excel",
            FileType::Csv => "CSV",
            FileType::Parquet => "Parquet",
            FileType::Arrow => "Arrow",
            FileType::Unknown => "Unknown",
        };
        write!(f, "{}", name)
    }
}

pub struct FileFinch;

impl FileFinch {
    pub fn detect(bytes: &[u8]) -> FileType {
        if let Some(file_type) = Self::detect_by_magic(bytes) {
            return file_type;
        }

        if let Ok(file_type) = Self::detect_geojson(bytes) {
            return file_type;
        }

        if Self::looks_like_csv(bytes) {
            return FileType::Csv;
        }

        FileType::Unknown
    }

    pub fn detect_from_path(path: &str, bytes: &[u8]) -> FileType {
        let detected = Self::detect(bytes);

        if detected != FileType::Unknown {
            return detected;
        }

        if let Some(extension) = std::path::Path::new(path)
            .extension()
            .and_then(|e| e.to_str())
        {
            match extension.to_lowercase().as_str() {
                "csv" => return FileType::Csv,
                "json" | "geojson" => {
                    if Self::detect_geojson(bytes).is_ok() {
                        return FileType::Geojson;
                    }
                }
                _ => {}
            }
        }

        FileType::Unknown
    }

    fn detect_by_magic(bytes: &[u8]) -> Option<FileType> {
        match bytes {
            [0x50, 0x4B, 0x03, 0x04, rest @ ..] => Self::detect_zip_content(rest),
            [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1, ..] => Some(FileType::Excel),
            [0x50, 0x41, 0x52, 0x31, ..] => Some(FileType::Parquet),
            bytes if bytes.starts_with(b"SQLite format 3\x00") => Some(FileType::Geopackage),
            bytes if bytes.starts_with(b"ARROW1") => Some(FileType::Arrow),
            bytes if Self::is_arrow_ipc_stream(bytes) => Some(FileType::Arrow),
            _ => None,
        }
    }

    fn detect_zip_content(bytes: &[u8]) -> Option<FileType> {
        let excel_patterns: &[&[u8]] = &[
            b"xl/worksheets",
            b"xl/_rels",
            b"docProps/",
            b"[Content_Types]",
            b"xl/workbook",
            b"xl/styles",
            b"xl/theme",
            b"xl/strings",
            b"xl/charts",
            b"xl/drawings",
            b"xl/sharedStrings",
            b"xl/metadata",
            b"xl/calc",
        ];

        let shapefile_patterns: &[&[u8]] = &[b".shp", b".dbf", b".prj", b".shx"];

        let is_excel = excel_patterns
            .iter()
            .any(|&pattern| bytes.windows(pattern.len()).any(|window| window == pattern));

        let is_shapefile = shapefile_patterns
            .iter()
            .any(|&pattern| bytes.windows(pattern.len()).any(|window| window == pattern));

        match (is_excel, is_shapefile) {
            (true, false) => Some(FileType::Excel),
            (false, true) => Some(FileType::Shapefile),
            _ => None,
        }
    }

    fn detect_geojson(bytes: &[u8]) -> Result<FileType, ()> {
        if let Ok(text) = std::str::from_utf8(bytes) {
            let text_lower = text.trim_start().to_lowercase();

            if text_lower.starts_with("{")
                && text_lower.contains(r#""type""#)
                && (text_lower.contains(r#""featurecollection""#)
                    || text_lower.contains(r#""feature""#)
                    || text_lower.contains(r#""geometry""#))
            {
                return Ok(FileType::Geojson);
            }
        }
        Err(())
    }

    fn looks_like_csv(bytes: &[u8]) -> bool {
        if bytes.is_empty() {
            return false;
        }

        if let Ok(text) = std::str::from_utf8(bytes) {
            let sample = if text.len() > 1000 {
                &text[..1000]
            } else {
                text
            };

            let lines: Vec<&str> = sample.lines().take(5).collect();
            if lines.is_empty() {
                return false;
            }

            let delimiter_counts: Vec<usize> =
                lines.iter().map(|line| line.matches(',').count()).collect();

            if delimiter_counts.is_empty() {
                return false;
            }

            let first_count = delimiter_counts[0];
            first_count > 0 && delimiter_counts.iter().all(|&count| count == first_count)
        } else {
            false
        }
    }

    fn is_arrow_ipc_stream(bytes: &[u8]) -> bool {
        if bytes.len() < 8 {
            return false;
        }

        let continuation = [0xFF, 0xFF, 0xFF, 0xFF];
        if bytes[4..8] == continuation && bytes[0..4] == [0x00, 0x00, 0x00, 0x00] {
            return true;
        }

        if bytes.len() >= 8 {
            let message_length = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            let metadata_length = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);

            if (8..0x100000).contains(&message_length)
                && metadata_length > 0
                && metadata_length < message_length
                && (message_length as usize) <= bytes.len()
                && bytes.len() > 8
                && !bytes[8..].starts_with(b"{")
                && !bytes[8..].starts_with(b"\"")
            {
                return true;
            }
        }

        false
    }

    pub fn analyze_data_format(&self, data: &[u8]) {
        let has_flatbuffer_header = data.len() >= 8;
        let message_length = if has_flatbuffer_header {
            u32::from_le_bytes([data[0], data[1], data[2], data[3]])
        } else {
            0
        };

        println!("Data analysis:");
        println!("Size: {} bytes", data.len());
        println!("Has FlatBuffer header: {}", has_flatbuffer_header);
        if has_flatbuffer_header {
            println!("Message length: {} bytes", message_length);
        }
        println!("First 16 bytes: {:02X?}", &data[0..data.len().min(16)]);
        if data.len() > 16 {
            println!("Last 16 bytes: {:02X?}", &data[data.len() - 16..]);
        }

        if data.starts_with(b"ARROW1") {
            println!("Arrow IPC File format detected (starts with ARROW1 magic)");
        } else if Self::is_arrow_ipc_stream(data) {
            println!("Arrow IPC Stream format detected (FlatBuffer header)");
            if data.len() >= 8 {
                let metadata_length = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
                println!("Metadata length: {} bytes", metadata_length);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_excel_xlsx() {
        let xlsx_header = vec![0x50, 0x4B, 0x03, 0x04];
        let mut bytes = xlsx_header;
        bytes.extend_from_slice(b"some data xl/worksheets more data");

        assert_eq!(FileFinch::detect(&bytes), FileType::Excel);
    }

    #[test]
    fn test_detect_excel_xls() {
        let xls_header = vec![0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
        assert_eq!(FileFinch::detect(&xls_header), FileType::Excel);
    }

    #[test]
    fn test_detect_parquet() {
        let parquet_header = vec![0x50, 0x41, 0x52, 0x31];
        assert_eq!(FileFinch::detect(&parquet_header), FileType::Parquet);
    }

    #[test]
    fn test_detect_geopackage() {
        let mut gpkg_header = b"SQLite format 3\x00".to_vec();
        gpkg_header.extend_from_slice(&[0; 100]);
        assert_eq!(FileFinch::detect(&gpkg_header), FileType::Geopackage);
    }

    #[test]
    fn test_detect_shapefile() {
        let mut shp_zip = vec![0x50, 0x4B, 0x03, 0x04];
        shp_zip.extend_from_slice(b"some data test.shp more data");
        assert_eq!(FileFinch::detect(&shp_zip), FileType::Shapefile);
    }

    #[test]
    fn test_detect_geojson() {
        let geojson = br#"{"type":"FeatureCollection","features":[]}"#;
        assert_eq!(FileFinch::detect(geojson), FileType::Geojson);
    }

    #[test]
    fn test_detect_csv() {
        let csv_data = b"name,age,city\nJohn,30,NYC\nJane,25,LA\n";
        assert_eq!(FileFinch::detect(csv_data), FileType::Csv);
    }

    #[test]
    fn test_detect_arrow_ipc_file() {
        let arrow_file = b"ARROW1\x00\x00";
        assert_eq!(FileFinch::detect(arrow_file), FileType::Arrow);
    }

    #[test]
    fn test_detect_arrow_ipc_stream() {
        let mut arrow_stream = vec![0x10, 0x00, 0x00, 0x00];
        arrow_stream.extend_from_slice(&[0x08, 0x00, 0x00, 0x00]);
        arrow_stream.extend_from_slice(&[0x00; 8]);
        assert_eq!(FileFinch::detect(&arrow_stream), FileType::Arrow);
    }

    #[test]
    fn test_detect_arrow_continuation() {
        let mut arrow_continuation = vec![0x00, 0x00, 0x00, 0x00];
        arrow_continuation.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
        assert_eq!(FileFinch::detect(&arrow_continuation), FileType::Arrow);
    }

    #[test]
    fn test_detect_unknown() {
        let random_bytes = vec![0x12, 0x34, 0x56, 0x78];
        assert_eq!(FileFinch::detect(&random_bytes), FileType::Unknown);
    }

    #[test]
    fn test_detect_from_path_csv() {
        let bytes = std::fs::read(
            "Downloads/OS_Open_Built_Up_Areas_GeoPackage/os_open_built_up_areas.gpkg",
        )
        .unwrap();
        assert_eq!(
            FileFinch::detect_from_path(
                "Downloads/OS_Open_Built_Up_Areas_GeoPackage/os_open_built_up_areas.gpkg",
                &bytes
            ),
            FileType::Geopackage
        );
    }
}
