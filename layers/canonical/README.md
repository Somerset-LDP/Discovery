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

## Environment variables

* `INPUT_LOCATION` - path to input file (likely this will point to a path in the pseudonmisation layer that holds psedunymised GP records in EMIS format)
* `OUTPUT_DB_HOST` - host name of the database server
* `OUTPUT_DB_PORT` - port that the database server is listening on (defaults to 5432)
* `OUTPUT_DB_NAME` - the name of the database to connect to (defaults to ldp)
* `OUTPUT_DB_USERNAME_SECRET` - the name of the secret holding the output database username eg canonical_layer/db_user_name
* `OUTPUT_DB_PASSWORD_SECRET` - the name of the secret holding the output database password eg canonical_layer/db_user_password
* `LOG_LEVEL` - an optional variable that alters the default log level of `INFO`. You must supply a valid log level for the Python logging library i.e. `CRITICAL`, `FATAL`, `ERROR`, `WARNING`, `INFO` or `DEBUG`

For testing the `OUTPUT_DB_USERNAME` and `OUTPUT_DB_USERNAME` variables can be set instead of the `*_SECRET` equivalents but this is not recommended for production