# To-do list

Legend  
:arrow_up:  Higher Priority  
:arrow_down: Lower Priority

## Basic Setup
- [x] Basic implementation of table namespace: C:03/02/2020
- [x] Basic Configuration: C:03/03/2020
- [x] Basic Operator Registration: C:03/04/2020
- [x] Package configuration: C:03/03/2020
- [x] Basic Documentation: C:03/04/2020
- [x] Prettify outputs and update documentation: C:03/04/2020
- [x] Validate mason configuration file using json_schema: C:03/04/2020
- [x] Validate operators according to json_schema C:03/04/2020
- [x] Add logger with log levels. C:03/05/2020
- [x] :arrow_up: Validate client compatability with operators C:03/06/2020
- [x] :arrow_up: Catch up old rest api interface (migrate https://github.com/samtecspg/data/tree/master/catalog/api to operators): C:/03/09/2020
- [x] :arrow_up: Move over tests and mocks.  C: 03/11/2020
- [ ] :arrow_up: More test coverage on basic funcitonality:
    - [x] Parameters C: 03/11/2020
    - [x] Configurations C:03/11/2020 
    - [x] Operators C: 03/12/2020
    - [x] Engines C: 03/13/2020
    - [x] Clients C: 03/23/2020
    - [ ] Cli (started, progress made)
- [x] :arrow_up: Clean up rest api implementation C:03/13/2020
- [x] create "mason run" cli command C: 03/09/2020
- [ ] Pull rest api responses through to swagger spec (200 status example)
- [ ] :arrow_up: Advanced Operator Registration Documentation 
- [ ] :arrow_up: New Client Documentation 
- [ ] :arrow_down: New Engine Documentation 
- [x] :arrow_up: Dockerize mason implementation C: 03/09/2020
- [x] Build and refine "Engines" first order concept C: 03/06/2020
- [ ] Establish docker style sha registration for installed operators to fix version conflicts  
- [x] ~~Explore graphql for the api? Note found a way around this for now.~~ wont do
- [ ] Generalize Engines to be "registerable" and serial
- [ ] Support multiple clients for a single engine type.
- [ ] Parameter aliases:   ex: database_name -> bucket_name
- [x] Establish common interfaces for metastore engine objects.   Metastore engine models, IE Table, Database, Schedule, etc C: 03/20/2020
- [ ] Allow operator definitions to have valid "engine set" configurations C:03/22/2020
- [x] Allow for multiple configurations C: 04/08/2020
- [ ] Clean up multiple configurations -> add id, don't use enumerate
- [ ] Allow operators to only be one level deep, ie not have a namespace (both in definition and folder configuration)
- [ ] Establish session for current configuration
- [ ] Add redis or sqllite for session 
- [ ] Move operator/configs over to redis
- [X] :arrow_up: Consolidate response.add_  actions and logger._ actions into one command C: 04/13/2020

## Test Cases

- [x] Malformed Parameters, extraneous ":".   Improve parameter specification.  Make docs more explicit C: 03/11/2020
- [x] Extraneous parameters.  Showing up in "required parameters" error return incorrectly. C: 03/11/2020
- [x] Better errors around Permission errors C: 03/13/2020


## Execution Engine
- [ ]  Look into using calcite or coral to extend spark operators to presto and hive
- [ ]  Look into using protos to communicate metastore schema to execution engine or possibly look into other serialization formats (avro)

## Operators
- [ ] Metastore Database operator
    - [ ] List databases (~= s3 list buckets)
- [x] Schema merge operator C:09/04/2020
- [ ] JSON explode operator
- [ ] S3 -> ES egress operator 
- [ ] Dedupe Operator
- [ ] Table Operators
    - [ ] Delete
    - [ ] Delete Database
- [ ] Seperate out database operator?
- [ ] Jobs operators (scheduler):
    - [x] Get C: 04/08/2020
    - [ ] List
- [ ] Scheduler operators:
    - [ ] List
    - [ ] Delete

## Clients

### Metastore
#### Glue
- [x] Basic setup. C: 4/04/2020
- [x] Fix conflicting schemas error with differing partition data. C: 04/2020
#### Hive
- [ ] :arrow_down: Basic setup
#### S3
- [x] :arrow_up: Basic Setup C: 3/20/2020
- [x] Schema implementations
   - [x] ParquetSchema C: 3/17/2020
   - [X] CSV Schema C: 4/16/2020
   - [ ] JsonL schema
   - [ ] Json schema
   - [ ] :arrow_down: Avro schema
   - [ ] :arrow_down: Msgpack pack schema

#### Python
- [ ] :arrow_up: Basic setup
#### IPython/Jupyter
- [ ] :arrow_up: Basic setup
- [ ] :arrow_up: Papermill integration 
#### Athena
- [ ] :arrow_up: Basic setup 
#### Spark
- [x] Basic setup 
    - [x] Kubernetes Operator Runner C: 4/06/2020
    - [ ] EMR Runner 
    - [ ] Local Runner
- [ ] Check that file format is supported
#### Presto
- [ ] :arrow_up: Basic setup 

### Dask
- [ ] Basic Setup
    - [ ] Kubernetes Runner

### Scheduler
### Glue
- [x] Basic set up
#### Airflow
- [ ] :arrow_up: Basic setup 

### Storage
#### Redshift
#### Elasticsearch
#### S3
- [ ] :arrow_up: Basic Setup 
- [ ] Redshift
- [ ] Elasticsearch

## :arrow_down: Preparing for public
- [ ] Remove samtec specific examples from examples/ files.  Use public examples


