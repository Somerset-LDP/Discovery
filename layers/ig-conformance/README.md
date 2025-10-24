# IG Conformance layer
Data stored in the IG conformance layer is derived from raw data. In order for raw data to be stored in the IG conformance layer it flows through the following steps

* only records for patient's who are in the cohort are retained, all other records are discarded
* records that are retained have their special category data e.g. Ethnicity replaced with a synthetic equivalent

Once the pipeline has finished processing a raw data set that data set is deleted from the LDP. Additionally the data stored in the IG conformance layer is expected to be short lived and to be deleted as soon as it has been processed by the next layer - [Pseudonymised](../pseudonymised//README.md)

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
|    └─ Data ingestion pipelines, one per feed e.g. EMIS feed to ingest raw GP data
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
  -t emis_gprecord:latest \
  -f aws/lambdas/emis_gprecord/Dockerfile .
```

Smoke testing the image- 
```bash
docker run -d --platform linux/amd64 -p 9000:8080 emis_gprecord:latest

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
  -t emis_gprecord:latest \
  -f aws/lambdas/emis_gprecord/Dockerfile .
```