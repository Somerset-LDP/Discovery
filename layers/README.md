# Layers

This is where all data ingestion pipelines live.

Each layer in LDP's storage model has a corresponding directory i.e.

```
layers/
├── ig-conformance/ 
│   └─ holds data that meets IG requirements
├── pseudonymised/
│   └─ holds data where PII has been pseudonymised
├── canonical/
    └─ holds data that has been standardised into a canonical model 
```

The pipelines under each dir are intended to output data into the layer that the dir is named after. As an example all pipelines under the `pseudonymised` dir should write data to the pseudonymised layer.

**More info -**
* [IG Conformance layer](./ig-conformance/README.md)
* [Pseudonymised layer](./pseudonymised/README.md)
* [Canonical layer](./canonical/README.md)

## Pipeline naming convention
Each layer holds the pipelines that are designed to feed that layer. Each pipeline script should ideally follow the naming convention `[source system]_[source entity].py`