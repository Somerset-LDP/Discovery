# Ethnicity Technical Test - terminology mapping

This project demonstrates how to manage and translate between different ethnicity code systems using a [FHIR terminology server](https://r4.fhir.space/terminology-module.html). It is based on the [HAPI-FHIR server](https://hapifhir.io/hapi-fhir/) and demonstrates the feasibility of representing hierarchical ethnicity classifications and performing code system translations using FHIR.

## Background

Healthcare systems often use different code systems to represent ethnicity, leading to challenges in data interoperability and analytics. This technical test aims to:

- Demonstrate how a FHIR terminology server can manage multiple ethnicity code systems (e.g., ONS, NHS).
- Show translation between code systems using FHIR ConceptMap resources.
- Represent the hierarchical nature of ethnicity classifications using FHIR CodeSystem and ValueSet resources.

## The Terminology Server

[HAPI-FHIR](https://hapifhir.io/hapi-fhir/) is being used as a terminology server for this technical test. Other solutions were considered however HAPI-FHIR was the only offering that allows full management of terminology resources through the standard FHIR specification (R4 release)

### Other terminology solutions that were ruled our

- [Snowowl](https://github.com/b2ihealthcare/snow-owl) - The FHIR API is read-only on the community edition
- [TermX](https://tutorial.termx.org/) - In the limited time available for this project TermX's [authentication layer](https://gitlab.com/kodality/terminology/termx-server/-/blob/main/README.md#authentication) was a barrier to adoption
- [Snowstorm](https://github.com/IHTSDO/snowstorm/tree/master) - Support for the FHIR standard around terminology is incomplete [especially around ConceptMap resources](https://github.com/IHTSDO/snowstorm/blob/master/docs/using-the-fhir-api.md#loading-fhir-packages)

The terminology server is based on the [HAPI-FHIR starter project](https://github.com/hapifhir/hapi-fhir-jpaserver-starter) which uses Docker to spin up a stripped back minimal FHIR server with some key limitations (eg no security, no logging). This setup is not production ready. 

## Project structure

```
ethnicity/
├── README.md
│   └─ Project documentation and usage instructions.
├── bin/
│   └─ Utility scripts for terminology server management.
├── terminology-resources/
│   └─ FHIR terminology resources (CodeSystem, ValueSet, ConceptMap).
└── terminology-server/
    └─ Docker-based configuration for the local HAPI-FHIR terminology server.
```

## Prerequisites

- [Docker Compose](https://docs.docker.com/compose/) installed on your machine.

## Deployment

### Start the terminology server

```sh
cd terminology-server
docker-compose up -d
```

### Load FHIR resources
You only need to do this once in order to seed the database. If you try to run it again after resources have been loaded you'll likely see errors with messages along the lines of "resource already exists".

Once the terminology server is up (if you're able navigate to [http://localhost:8080/](http://localhost:8080/) then the server is ready to accept requests) then you can load in the terminology resources that showcase the proposed approach to ethnicity code system management. Make sure the [`load_resources.sh`](./bin/load_resources.sh) script is executable (`chmod +x load_resources.sh`)

   ```sh
   cd bin
   load_resources.sh
   ```

## Terminology

All terminology resources can be viewed in the HAPI-FHIR server once deployment is complete. However you can also look at the raw files that live under [terminology-resources/](./terminology-resources/)

### CodeSystems

The following code systems are available in this demo. Note that they only contain ethnicity codes for this demo. In reality there would contain hundred if not thousands of different codes.

- [nhs_data_dictionary.json](./terminology-resources/codesystem/nhs_data_dictionary.json)
- [ons_data_dictionary.json](./terminology-resources/codesystem/ons_data_dictionary.json) 

### ValueSets

The declaration of these ValueSets in terms of which codes from the CodeSystem are in scope is not robust. We can get away with including all codes from the respective CodeSystem in our contrived examples. This is because our NHS and ONS code systems only contain ethnicity codes. 

In reality a code system can have hundreds or even thousands of codes, many of which may not be appropriate for your use case. A ValueSet is there to draw a boundary around part of the code system e.g. a value set holding all of the ethnicity related codes from a code system.

- [ons_data_dictionary_ethnicity_2021.json](./terminology-resources/valueset/ons_data_dictionary_ethnicity_2021.json) is made up of the [Office for National Statistics ethnicity codes as used in the 2021 England & Wales census.](https://www.ons.gov.uk/census/census2021dictionary/variablesbytopic/ethnicgroupnationalidentitylanguageandreligionvariablescensus2021/ethnicgroup/classifications) 
- [nhs_data_dictionary_ethnicity_2001a.json](./terminology-resources/valueset/nhs_data_dictionary_ethnicity_2001a.json) is made up of the [National Health Service ethnicity codes based on 2021 England & Wales census classification.](https://data.developer.nhs.uk/specifications/NHS-CDA-eDischarge/Vocabulary/Ethnicity.html) 

We also have a number of value sets that are there to expand a particular branch of the ONS ethnicity hierarchy. In practice this could be used to retrieve all ancestor codes under a given code. This would support aggregation calculations which need the ancestor/leaf codes in order to count up instances in a given data set.

### ConceptMaps

Mappings between our code systems are provided using FHIR ConceptMap resources. There are two maps available - 

- [nhs_2001a_ethnicity_to_ons2021_ethnicity.json](./terminology-resources/conceptmap/nhs_2001a_ethnicity_to_ons2021_ethnicity.json) - Ambiguity arises when converting from the flatter NHS to the richer ONS CodeSystem for example NHS 2001a has a single code C: Any other White background. In ONS 2021, this could mean “White: Gypsy or Irish Traveller”, “White: Roma”, or “Other White”. 
- [ons_2021_ethnicity_to_nhs_2001a_ethnicity.json](./terminology-resources/conceptmap/ons_2021_ethnicity_to_nhs_2001a_ethnicity.json) - support “downgrading” richer ONS categories into the flatter NHS 2001a model

## Usage

You can interact with the terminology server using FHIR's standard RESTful APIs. Remember that only CodeSystem, ValueSet and ConceptMap resources are available.

Two scripts have been created to allow you to explore some of the useful terminology features that a FHIR server offers. Call each script without parameters to see usage instructions. Before you attempt to run the scripts make sure that they are executable. 

 - [`valueset_expand.sh`](./bin/valueset_expand.sh) - expand a ValueSet to show the codes that it contains
 - [`code_translate.sh`](./bin/code_translate.sh) - show all of the mappings that exist for a given code

## Productionisation steps

This is not an exhaustive list. 

- Security 
  - Check base Docker images for security vulnerabilities
  - FHIR server should be inaccessible without AuthN/Z
- Assess scaling needs, 
  - Application layer - a single instance may not be acceptable
  - Data store - deploy a Postgres compatible service
  - Text search service - deploy an Elasticsearch service (or cluster) as opposed to local Apache Lucene
- Monitoring 
  - Enable logging

## Future direction

- [NHS England term server syndication](https://digital.nhs.uk/services/terminology-server#syndication) - this would allow us to leverage nationally maintained code systems and value sets like SNOMED-CT (convenient way to consume this licensed content)