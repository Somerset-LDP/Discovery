# Data Ingestion Lambda

Downloads reference data from external sources and uploads to S3 Landing zone.

## Overview

This function:

1. Receives event with data source type and target bucket
2. Downloads file from external URL (ONSPD or IMD)
3. Extracts content if needed (ZIP for ONSPD)
4. Uploads to S3 Landing bucket with date-partitioned path

## Processing Flow

```
External Source (HTTP) → Data Ingestion Lambda → S3 Landing
        ↓                                              ↓
   ONSPD (ZIP)                          landing/reference/{source}/YYYY/MM/DD/
   IMD (XLSX)
```

## Lambda Event Format

```json
{
  "data_source": "onspd",
  "s3_bucket": "ldp-zone-a-landing",
  "ingestion-timestamp": "2025-12-01T14:30:45.123456"
}
```

| Parameter             | Required | Description                              |
|-----------------------|----------|------------------------------------------|
| `data_source`         | Yes      | Source type: `onspd` or `imd_2019`       |
| `s3_bucket`           | Yes      | Target S3 bucket name                    |
| `ingestion-timestamp` | Yes      | ISO 8601 timestamp for path partitioning |

**Supported timestamp formats:**

- `2025-12-01`
- `2025-12-01T14:30:45`
- `2025-12-01T14:30:45.123456`
- `2025-12-01T14:30:45Z`

## Environment Variables

### ONSPD (Office for National Statistics Postcode Directory)

| Variable              | Required | Description                                                                       |
|-----------------------|----------|-----------------------------------------------------------------------------------|
| `ONSPD_URL`           | Yes      | URL to download ONSPD ZIP file                                                    |
| `ONSPD_TARGET_PREFIX` | Yes      | Path within ZIP to extract (e.g., `ONSPD_FEB_2024_UK/Data/ONSPD_FEB_2024_UK.csv`) |

### IMD (Index of Multiple Deprivation)

| Variable            | Required | Description                                                                 |
|---------------------|----------|-----------------------------------------------------------------------------|
| `IMD_URL`           | Yes      | URL to download IMD XLSX file                                               |
| `IMD_TARGET_PREFIX` | Yes      | Output filename (e.g., `File_1_IoD2025_Index_of_Multiple_Deprivation.xlsx`) |

## Data Sources

| Source   | Value      | Format             | Processing                                |
|----------|------------|--------------------|-------------------------------------------|
| ONSPD    | `onspd`    | ZIP containing CSV | Downloads to temp, extracts specific file |
| IMD 2019 | `imd_2019` | XLSX               | Downloads directly to memory              |

## S3 Output Path

**Pattern:**

```
s3://{s3_bucket}/landing/reference/{data_source}/YYYY/MM/DD/{filename}
```

**Examples:**

```
s3://ldp-zone-a-landing/landing/reference/onspd/2025/12/01/ONSPD_FEB_2024_UK.csv
s3://ldp-zone-a-landing/landing/reference/imd_2019/2025/12/01/File_1_IoD2025_Index_of_Multiple_Deprivation.xlsx
```

## Notes

- ONSPD ZIP files are large (~1,3GB) - uses temporary disk storage to minimise memory usage
- IMD XLSX files are small - downloaded directly to memory
- Temporary files are cleaned up after successful upload
- Date path components (YYYY/MM/DD) extracted from `ingestion-timestamp`

