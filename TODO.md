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
    - [ ] Configurations
    - [ ] Operators
    - [ ] Engines
    - [ ] Clients
    - [ ] Cli
- [ ] :arrow_up: Clean up rest api implementation, 
- [x] create "mason run" cli command C: 03/09/2020
- [ ] Pull rest api responses through to swagger spec (200 status example)
- [ ] :arrow_up: Advanced Operator Registration Documentation 
- [ ] :arrow_up: New Client Documentation 
- [ ] :arrow_down: New Engine Documentation 
- [x] :arrow_up: Dockerize mason implementation C: 03/09/2020
- [x] Build and refine "Engines" first order concept C: 03/06/2020
- [ ] Establish docker style sha registration for installed operators to fix version conflicts
- [ ] :arrow_down: Explore graphql for the api? Note found a way around this for now.
- [ ] Generalize Engines to be "registerable" and serial
- [ ] Support multiple clients for a single engine type.

## Test Cases

- [x] Malformed Parameters, extraneous ":".   Improve parameter specification.  Make docs more explicit C: 03/11/2020
- [x] Extraneous parameters.  Showing up in "required parameters" error return incorrectly. C: 03/11/2020
- [ ] Better errors around Permission errors

## Operators

- [ ] Schema merge operator
- [ ] JSON explode operator
- [ ] S3 -> ES egress operator 
- [ ] Dedupe Operator
- [ ] Table Operators
    - [ ] Delete
    - [ ] Delete Database
- [ ] Seperate out database operator?
- [ ] Jobs operators (scheduler):
    - [ ] Get
    - [ ] List
- [ ] Scheduler operators:
    - [ ] List
    - [ ] Delete

## Clients

### Metastore  
#### Glue
- [x] Basic setup. C: 4/04/2020
- [ ] Column casting operator
- [ ] Fix conflicting schemas error with differing partition data
#### Hive
- [ ] :arrow_down: Basic setup

### Execution
- [ ] :arrow_up: Dockerized Execution
- [ ] :arrow_up: Kubernetes Execution

#### Python
- [ ] :arrow_up: Basic setup
#### IPython/Jupyter
- [ ] :arrow_up: Basic setup
- [ ] :arrow_up: Papermill integration 
#### Athena
- [ ] :arrow_up: Basic setup 
#### Spark
- [ ] Basic setup 
#### Presto
- [ ] :arrow_up: Basic setup 

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


