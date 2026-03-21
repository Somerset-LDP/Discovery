# Synthetic Data Generator

## Overview

This project generates synthetic patient data using [Synthea](https://github.com/synthetichealth/synthea/wiki) (v 3.4.0) localised to England. Each patient is generated with one year of medical history data including encounters, observations and conditions. Data is outputted in FHIR R4 format. Note that there are some US extensions in the FHIR resources that Synthea generates. These are baked into Synthea's R4 exporter code. Note that it is also capable of outputting data in other formats.

Additionally it post-processes Synthea's output to generate real postcodes for each patient, something that Synthea does not do. 

It is designed to be run in a containerized environment.

## Build

To build the Docker image, run the following command from the project root (where the Dockerfile is located):

```bash
docker build -t synth-data-generator .
```

## Usage

### Container Command-Line Arguments

The following command-line arguments can be passed to the container to control data generation:

| Parameter         | Mandatory | Default    | Description |
|------------------ |-----------|------------|-------------|
| --region           | Yes       | South West | Generated patients and care providers will be from this region. Note that currently the only valid region is "South West".             |
| --populationSize  | Yes       | 1          | The number of synthetic patients to generate.             |
| --city            | No        | N.A.       | Generated patients will be from this city. Generated care providers may be from other locations. If the city is not provided then Synthea will pick a random city from the region. The city must be known to Synthea. For a set of valid cities open the [demographics.csv](./synthea/england/resources/geography/demographics.csv) file and pick a value from the `NAME` column.             |
| --help            | No        | N.A.       | Show usage information and exit.                                                 |
| --serialisers           | No       | N.A. | A comma separated list of additional formats (FHIR is always generated) that the data should be written in. Currently only Somerset Foundation Trust format is supported (sft)             |

### Environment Variables

- `LOG_LEVEL` (optional): Set the logging level for the application (e.g., `INFO`, `DEBUG`, `ERROR`). Default is `ERROR`.

### Mounting the Output Directory

The container exits once synthetic data has been generated. To persist the data to your host machine, mount a local directory to the container's output directory using the `-v` (volume) flag. Note that the container directory path is always `/synthetic-data-generator/output`:

```bash
docker run --rm -v $(pwd)/output:/synthetic-data-generator/output synth-data-generator
```

### Example Commands

Run with default parameters (generates one patient from a random city in the South West of England):

```bash
docker run --rm -v $(pwd)/output:/synthetic-data-generator/output synth-data-generator
```

Run with custom parameters:

```bash
docker run --rm -v $(pwd)/output:/synthetic-data-generator/output \
  -e LOG_LEVEL=INFO \
  synth-data-generator --state "South West" --populationSize 10 --city "Dorchester" --serlialisers sft
```

Show help:

```bash
docker run --rm synth-data-generator --help
```

### Adding a serialiser
Serialisation is triggered from the `_serialise` function in the [generate.py](./generate.py) script. To support a new serialisation format it is a case of creating a function that takes two parameters

1. `fhir_bundle_files: list` - a list of files in FHIR R4 format whose contents are transformed into the target format
2. `output_dir: str` - a location where the target data will be written to

The new serialiser function should then be registered in the `_serialise` function by adding a reference to the serialiser function to the `serialiser_funcs` dict. Note that the key for the dict will be the value used on the command line e.g. if the serialiser function is registered with the key "foo" then to invoke it via the command line the end user would use something like -

```bash
docker run synth-data-generator  --serlialisers foo
```

**Note** that placeholder functions have been added to the [generate.py](./generate.py) script for council (`_to_council`) and EMIS (`_to_emis`) data.