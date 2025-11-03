# Canonical layer

**Purpose**: The source of truth for clean, consistent, canonical data models. This is the business-ready foundation for all downstream analytics and processing.

**Characteristics**:
- **Conflict resolution**: When multiple feeds provide conflicting data, business rules determine truth
- **Canonical models**: Standard patient, encounter, observation entities reflecting business semantics
- **Data quality enforcement**: Type validation, reference integrity, consistency rules
- **Feed-agnostic**: Data structure no longer tied to source system quirks
- **FHIR integration**: Code system validation and terminology mapping

**Storage**: Relational database with normalized schema

## Project strucutre
```
ig-conformance/
├── README.md
│   └─ Project documentation and usage instructions.
├── aws/
│   └─ AWS specific code e.g. Lambdas to run pipelines in an AWS environment
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
  -t emis_gprecord_canonical:latest \
  -f Dockerfile .
```

Smoke testing the image- 
```bash
docker run -d --platform linux/amd64 -p 9000:8080 emis_gprecord_canonical:latest

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
  -t emis_gprecord_canonical:latest \
  -f Dockerfile .
```