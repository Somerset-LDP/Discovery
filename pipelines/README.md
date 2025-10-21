# Pipelines

This is where all data ingestion pipelines live.

Each layer in LDP's storage model has a corresponding directory i.e.

```
pipelines/
└── ig-conformance/
└── pseudo/
└── canonical/
```

The pipelines under each dir are intended to output data into the layer that the dir is named after. As an example all pipelines under the `pseudo` dir should write data to the pseudo layer.

Within each dir the individual pipeline scripts can be found. They should be named for `[source system]_[source entity].py`