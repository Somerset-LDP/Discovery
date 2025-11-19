# Patient Services — Linking, Matching & Verification

This module contains functionality for patient identity resolution within the platform.  
It supports:

- **Synchronous patient linking** for ingestion pipelines  
- **Asynchronous patient verification** using remote MPI responses  
- **Local and (future) remote MPI abstractions**  
- A clean separation between AWS Lambda handlers and domain logic

The structure is designed so each functional area can be deployed independently while sharing a consistent patient-matching domain.

---

## Folder Structure

```text
patient/
│
├─ linking/
│   ├─ aws/lambda/handler.py         # Synchronous Lambda entrypoint
│   ├─ service.py                    # Linking orchestration
│   └─ matchers.py                   # Strict/fuzzy/probabilistic matching
│
├─ verification/
│   ├─ aws/lambda/handler.py         # Event-driven Lambda
│   └─ verifier.py                   # Reconcile unverified → verified
│
└─ mpi/
    ├─ local/
    │   └─ repository.py             # Access to local MPI/CDM
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

## 1. Linking Service (`linking/`)

Provides *synchronous* patient linking during ingestion.  
Returns a stable patient identifier (verified or temporary) so ingestion can continue without waiting for remote MPI lookups.

### `aws/lambda/handler.py`
- AWS-facing wrapper.
- Translates event → Python call into `service.py`.
- Contains no business logic.

### `service.py`
- Orchestrates patient linking.
- Performs:
  - Local MPI lookup
  - Invocation of matching algorithms
  - Creation of temporary unverified patients
  - Returning the correct patient reference to the caller

### `matchers.py`
- Encapsulates match algorithms (strict, fuzzy, probabilistic in future).
- Keeps algorithmic logic separated from orchestration.

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

## Current Implementation Focus

This phase delivers:

1. **Linking service (synchronous)**  
2. **Verification service (asynchronous)**  
3. **Local MPI repository**  
4. **Stub async PDS request/response structure** (no real integration)

The structure is ready for full expansion without future refactoring.

---

## Productionisation steps

patient/linking/service.py

Sex class should be externalised into the Termonology server. There should be a local global cache. We should use the LDP's canonical model of Sex.