# Patient Services — Matching & Verification

This module contains functionality for patient identity resolution within the platform.  
It supports:

- **Synchronous patient matching** for ingestion pipelines  
- **Asynchronous patient verification** using remote MPI responses  
- **Local and (future) remote MPI abstractions**  
- A clean separation between AWS Lambda handlers and domain logic

The structure is designed so each functional area can be deployed independently while sharing a consistent patient-matching domain.

---

## Folder Structure

```text
patient/
│
├─ matching/
│   ├─ aws/lambda/handler.py         # Synchronous Lambda entrypoint
│   ├─ service.py                    # Matching orchestration
│   └─ matchers.py                   # Strict/fuzzy/probabilistic matching
│
├─ verification/
│   ├─ aws/lambda/handler.py         # Event-driven Lambda
│   └─ verifier.py                   # Reconcile unverified → verified
│
└─ mpi/
    ├─ local/
    │   └─ repository.py             # Access to local MPI/CDM
│   |   └─ matching.py               # Matching strategy (Strict/fuzzy/probabilistic)
    │
    └─ pds/async/
        ├─ request/
        │   ├─ aws/lambda/handler.py # Timer-triggered submitter
        │   ├─ client.py             # Batch submission orchestration
        │   └─ batch.py              # In-memory/on-disk batching model
        │
        └─ response/
            ├─ aws/lambda/handler.py # Timer-triggered poller
            └─ trace.py              # Response processing & pseudonymisation
```


---

## 1. Matching Service (`matching/`)

Provides *synchronous* patient matching during ingestion.  
Returns a stable patient identifier (verified or temporary) so ingestion can continue without waiting for remote MPI lookups.

### `aws/lambda/handler.py`
- AWS-facing wrapper.
- Translates event → Python call into `service.py`.
- Contains no business logic.

### `service.py`
- Orchestrates patient matching.
- Performs:
  - Local MPI lookup
  - Invocation of matching algorithms
  - Creation of temporary unverified patients
  - Returning the correct patient reference to the caller

---

## 2. Verification Service (`verification/`)

Runs *asynchronously* when a remote MPI response is available.

### `aws/lambda/handler.py`
- Triggered by an event (SNS/EventBridge).
- Forwards the event into `verifier.py`.

### `verifier.py`
- Reconciles unverified patients with remote match results.
- Performs:
  - Promote/merge/update logic
  - Cleanup of rejected or ambiguous temporary patients
  - Local MPI writes via the repository

---

## 3. MPI Layer (`mpi/`)

Centralised abstraction for local and remote MPI interactions.

### `local/repository.py`
- Encapsulates access to the local MPI/CDM.
- Handles:
  - Patient search
  - Creation of unverified patients
  - Updating and promotion
  - Merging and deletion

### `local/matching.py`
- Encapsulates match algorithms (strict, fuzzy, probabilistic in future).
- Keeps algorithmic logic separated from orchestration.

### Database

The `local/data/migrations` directory contains the SQL scripts for bootstrapping the local MPI database. The intention is that these files will be run in order against a blank database to bring it up to the current version of the local MPI schema. Alternatively the execution could begin at a specified file to bring an existing database up to date.

Note that the database, schema and users are expected to be created elsewhere and are not the concern of this module.

The file names follow the convention `<migration_time_in_UTC>__<short_action_description>.sql`. By prefixing with the timestamp of when the migration the order in which the files should be run is implicit i.e. oldest to newest.

The short_action_description should follow the form `verb_object_detail`

**verb** - ∈ {create, add, alter, rename, drop, delete, update}

| Verb     | When to use                                |
| -------- | ------------------------------------------ |
| `create` | New table, index, constraint               |
| `add`    | Adding column, index, constraint           |
| `alter`  | Changing column type, nullability, default |
| `rename` | Renaming table or column                   |
| `drop`   | Removing table, column, index, constraint  |
| `update` | Modifying reference or seed data           |
| `delete` | Removing seed/test/reference data          |

**object** - ∈ {table, column, index, constraint, fk, pk, enum, view, data}

**detail** - describes the specific target e.g. - 

* create_table_patient
* add_column_patient_dob
* alter_column_appointment_status_type
* rename_table_maternity_case_to_birth_record
* drop_index_patient_lastname
* add_fk_patient_gp_id
* update_data_reference_status_codes
* delete_data_test_records

---

## 4. PDS Async Layer (`mpi/pds/async/`)

Provides the structure for remote MPI / PDS integration.  
**This is currently a placeholder** — the implementation is deferred until PDS access is available.

### `request/`
Handles batching of outbound PDS trace requests.

- `aws/lambda/handler.py`  
  Timer-triggered submitter Lambda.

- `client.py`  
  Determines when and how batches are submitted.

- `batch.py`  
  Tracks queued trace requests.

### `response/`
Handles scheduled polling of inbound PDS/DBS trace responses.

- `aws/lambda/handler.py`  
  Timer-triggered poller Lambda.

- `trace.py`  
  Discovers new responses, pseudonymises them, persists them.

---

## Development Notes

- Lambdas should be thin wrappers; business logic stays in service modules.
- Domain model classes will be added later as patterns emerge.
- Eventing concerns (SNS/EventBridge schemas, DLQs, etc.) deliberately remain outside the domain code.
- The PDS async folders provide a stable integration boundary, even though remote MPI connectivity is not yet available.

---

## AWS Lambdas

The project exposes two Lambdas both of which are deployed as Docker images - 

* Matching Service - intended to be used by Data pipelines. Returns a stable patient identifier (verified or temporary) so ingestion can continue without waiting for remote MPI lookups.
* Verification Service - runs asynchronously sweeping up temporary patient records marking them as verified where appropriate

### Matching Service 

#### Request/Response format

**Request**
```json
    {
        "patients": [
            {
                "nhs_number": "1234567890",
                "first_name": "John",
                "last_name": "Doe",
                "postcode": "SW1A 1AA",
                "dob": "1980-01-15",
                "sex": "male"
            }
        ]
    }
```

**Successful Response**

```json
    {
      "message": "Patient Linking completed successfully",
      "request_id": "<AWS request ID>",
      "counts": {
        "total": 1,
        "single": 0,
        "multiple": 1,
        "zero": 0        
      }
      "data": [
          {
              "nhs_number": "1234567890",
              "first_name": "John",
              "last_name": "Doe",
              "postcode": "SW1A 1AA",
              "dob": "1980-01-15",
              "sex": "male",
              "patient_ids": ["patient-id-1", "patient-id-2"]
          },
      ]
    }
```

**Error Response**

```json
    {
      "message": "Patient Linking Lambda execution failed",
      "request_id": "<AWS request ID>"
    }
```

#### Environment variables

* `MPI_DB_HOST` - host name of the MPI database server
* `MPI_DB_PORT` - port that the MPI database server is listening on (defaults to 5432)
* `MPI_DB_NAME` - the name of the MPI database to connect to (defaults to ldp)
* `MPI_SCHEMA_NAME` - the name of the MPI schema (defaults to mpi)
* `MPI_DB_USERNAME_SECRET` - the name of the secret holding the MPI database username eg canonical_layer/db_user_name
* `MPI_DB_PASSWORD_SECRET` - the name of the secret holding the MPI database password eg canonical_layer/db_user_password
* `LOG_LEVEL` - an optional variable that alters the default log level of `INFO`. You must supply a valid log level for the Python logging library i.e. `CRITICAL`, `FATAL`, `ERROR`, `WARNING`, `INFO` or `DEBUG`

For testing the `MPI_DB_USERNAME` and `MPI_DB_USERNAME` variables can be set instead of the `*_SECRET` equivalents but this is not recommended for production

### Verification Service 
TODO

#### Environment variables


### Building the Docker image

For local development and testing -
```bash
docker buildx build \
  --platform linux/amd64 \
  --provenance=false \
  -t [service-name]:latest \
  -f patient/[service-name]/aws/lambda/Dockerfile .
```

Smoke testing the image- 
```bash
docker run -d --platform linux/amd64 -p 9000:8080 [service-name]:latest

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
  -t patient-[service]:latest \
  -f patient/[service]/aws/lambda/Dockerfile .
```

## Current Implementation Focus

This phase delivers:

1. **Matching service (synchronous)**  
2. **Verification service (asynchronous)**  
3. **Local MPI repository**  
4. **Stub async PDS request/response structure** (no real integration)

The structure is ready for full expansion without future refactoring.

---

## Productionisation steps

patient/linking/service.py

Sex class should be externalised into the Termonology server. There should be a local global cache. We should use the LDP's canonical model of Sex.