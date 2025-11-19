# IG Conformance layer
Data stored in the IG conformance layer is derived from raw data. In order for raw data to be stored in the IG conformance layer it flows through the following steps

* only records for patient's who are in the cohort are retained, all other records are discarded
* records that are retained have their special category data e.g. Ethnicity replaced with a synthetic equivalent

Once the pipeline has finished processing a raw data set that data set is deleted from the LDP. Additionally the data stored in the IG conformance layer is expected to be short lived and to be deleted as soon as it has been processed by the next layer - [Pseudonymised](../pseudonymised//README.md)

## Step Function Integration

The Lambda is designed to be triggered by with required event parameters:

```json
{
  "input_path": "s3://bucket/path/to/input/file.csv",
  "output_path": "s3://bucket/output",
  "feed_type": "gp"
}
```

**Parameters:**
- `input_path` (required) - Full S3 path to input file
- `output_path` (required) - Base S3 path for output files
- `feed_type` (required) - Feed type: `gp` or `sft`

**Feed differences:**
- GP: NHS number in column 0, 2 metadata rows, metadata preserved in output
- SFT: NHS number in column 1, no metadata rows, no metadata in output

Output structure: `{output_path}/{feed_type}_feed/YYYY/MM/DD/original_filename.csv`

## Project strucutre
```
ig-conformance/
├── README.md
│   └─ Project documentation and usage instructions.
├── aws/
│   └─ AWS specific code e.g. Lambdas to run pipelines in an AWS environment
├── common/
│   └─ Shared code that can be used across the project e.g. cohort membership filtering.
├── pipeline/
|    └─ Data ingestion pipelines
└── tests/
    └─ Unit and Integration tests built with Pytest
```

## Build & Test

### Prerequisites
- Docker with buildx support
- Python 3.12+
- pytest for running tests

## Building the Docker image

The Lambda function is packaged as a Docker container for deployment to AWS Lambda.

For local development and testing -
```bash
docker buildx build \
  --platform linux/amd64 \
  --provenance=false \
  -t ig_conformance:latest \
  -f Dockerfile .
```

Smoke testing the image- 
```bash
docker run -d --platform linux/amd64 -p 9000:8080 conformance_processor:latest

curl "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'
```

#### Corporate Network Build (ZScaler)
When building behind corporate firewalls or proxies, include SSL certificates:
Note that the secret id must be named `ssl-certs` and points to the path of your corporate SSL cert

```bash
docker buildx build \
  --secret id=ssl-certs,src=/etc/ssl/certs/ca-certificates.crt \
  --platform linux/amd64 \
  --provenance=false \
  -t ig_conformance:latest \
  -f Dockerfile .
```

## Environment variables

* `COHORT_STORE` - path to the cohort store
* `PSEUDONYMISATION_LAMBDA_FUNCTION_NAME` - the name of the Pseudonymisation Lambda that the IG conformance pipeline will use as part of the cohort checking step
* `KMS_KEY_ID` - the ARN of the KMS encryption key used when writing to the `OUTPUT_LOCATION`. 
* `LOG_LEVEL` - an optional variable that alters the default log level of `INFO`. You must supply a valid log level for the Python logging library i.e. `CRITICAL`, `FATAL`, `ERROR`, `WARNING`, `INFO` or `DEBUG`
* `PSEUDONYMISATION_BATCH_SIZE` - an optional variable to set the max number of NHS numbers (as an integer) that will be sent in a single request to the Pseudonymisation Lambda. Default value is 10000

For testing the `SKIP_ENCRYPTION` variable can be set to avoid calling out to the Psuedonymisation service. This is not recommended for production