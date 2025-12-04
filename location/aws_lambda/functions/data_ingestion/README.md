# Data Ingestion Lambda - Environment Variables

## Required Environment Variables

### ONSPD (Office for National Statistics Postcode Directory)

```bash
ONSPD_URL="https://www.arcgis.com/sharing/rest/content/items/6fb8941d58e54d949f521c92dfb92f2a/data"
ONSPD_TARGET_PREFIX="ONSPD_FEB_2024_UK/Data/ONSPD_FEB_2024_UK.csv"
```

### IMD (Index of Multiple Deprivation)

```bash
IMD_URL="https://assets.publishing.service.gov.uk/media/691dece32c6b98ecdbc500d5/File_1_IoD2025_Index_of_Multiple_Deprivation.xlsx"
IMD_TARGET_PREFIX="File_1_IoD2025_Index_of_Multiple_Deprivation.xlsx"
```

## Usage

### Lambda Event Structure

```json
{
  "data_source": "onspd",
  "s3_bucket": "ldp-zone-a-landing",
  "ingestion-timestamp": "2025-12-01T14:30:45.123456"
}
```

**Timestamp Format:**
The `ingestion-timestamp` field accepts various ISO 8601 formats:
- Date only: `2025-12-01`
- Date and time: `2025-12-01T14:30:45`
- Date, time and fractional seconds: `2025-12-01T14:30:45.123456`
- With trailing 'Z': `2025-12-01T14:30:45.123456Z`
- Space separator: `2025-12-01 14:30:45`

All formats are parsed to extract year, month, and day for the S3 path.

### S3 Output Path Format

```
s3://{s3_bucket}/landing/reference/{data_source}/YYYY/MM/DD/{filename}
```

**Example for ONSPD:**
```
s3://ldp-zone-a-landing/landing/reference/onspd/2025/12/01/ONSPD_FEB_2024_UK.csv
```

**Example for IMD:**
```
s3://ldp-zone-a-landing/landing/reference/imd_2019/2025/12/01/File_1_IoD2025_Index_of_Multiple_Deprivation.xlsx
```

## Data Sources

| Data Source | Value | Description |
|-------------|-------|-------------|
| ONSPD | `onspd` | Office for National Statistics Postcode Directory |
| IMD 2019 | `imd_2019` | Index of Multiple Deprivation 2019 |

## Notes

- **ONSPD**: Downloads ZIP file to temporary storage and extracts specific CSV file from path defined in `ONSPD_TARGET_PREFIX`
- **IMD**: Downloads XLSX file directly to memory (small file, no extraction needed)
- Date components (YYYY/MM/DD) are extracted from `ingestion-timestamp` (accepts various ISO 8601 formats)
- Filename in S3 key is extracted from the last component of `TARGET_PREFIX`
- Large files use temporary disk storage to minimise memory usage

