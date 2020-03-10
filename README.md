# Mason - Data Operator Framework ![Mason Logo](images/MasonLogo.png) 

Mason is the connecting tissue for your data projects.  You can think of Data Operators as a "data aware" analogue to the concept of an airflow operator, or a data analogue to the react.js concept of a "component".  In reality it specifies Data Operators which interact with 4 configuration abstractions called "Engines":

![Operator Engines](images/OperatorConfigs.png)

1.   Storage Engines - Any activity that involves serial (row level) access and storage of data.  Some example storage clients would be S3 or HDFS.
2.   Metastore Engines - Any activity that involves accessing metadata of datasets such as partitioning or schema information but not the data itself.  Some example metastore clients would be Glue, or Hive.
3.   Execution Engines - Any activity that involves programatic serial or SQL analytical computation on data.  Example exeuction engines would be spark, presto, or athena.
4.   Scheduler Engines -  Anything that involves scheduling frequency of data jobs.  Example scheduler clients would be airflow, or aws data pipelines

Mason is heavily inspired by language agnostic configuration driven tools like kubernetes, helm, and terraform.   Mason aims to help to make existing higher level open source big data tools _easier_ to coordinate with one another and make them easier to interact with for individuals of various expertise across organizations.  Mason does not attempt to make provisioning and standing up such services referenced in its configurations easier and thus is meant to be used in conjunction with tools
like kubernetes and helm.

Mason's mission is to provide ways to build composable self contained functional units called "Data Operators" which companies can stitch together to easily provide end to end data pipelines.   The target demographic of Mason are companies that are just breaking into the enterprise data space, or companies that are looking to consolidate their data operations.

## Quickstart
### Docker

If you are implementing aws clients remember to update `.env` file to include AWS credentials:
```.env
AWS_ACCESS_KEY_ID=<KEY_ID>
AWS_SECRET_ACCESS_KEY=<SECRET_KEY>
```
which have the needed permissions for the AWS services you are specifying as engine clients.  You can see all such configurations for various cloud providers in `.env.example`

Then run
```
./docker_build
```
Followed by
```
docker compose up
```

Swagger ui for registered operators can then be found at: `http://localhost:5000/api/ui/`  

You can access additional mason commands by shelling into the running docker container and running them via:

```
docker exec -it $(docker ps | grep mason | awk '{print $1}') /bin/bash
> root@<SHA>:/app# mason
Usage: mason [OPTIONS] COMMAND [ARGS]...

  Mason Data Operator Framework

Options:
  --help  Show this message and exit.

Commands:
  config
  operator
  register
  run

```

Note that the Dockerfile registers a set of example operators by default.  Comment out these lines and register different operators if you wish to do so during docker build.

### Python Package

To install the python `mason` package instead first run:
```
./scripts/install
```
Mason leverages `mypy` heavily for ensuring that function signatures and types are in line. Install will run mypy and stop if it does not succeed.  

### Basic Mason Commands

To configure mason run `mason config`.  Configurations are validated for basic structure using json_schema.  See `configurations/schema.json`:
```
mason config examples/config/config_example.yaml
>>
Creating MASON_HOME at ~/.mason/
Creating OPERATOR_HOME at ~/.mason/operators/

Using config examples/config/config_example.yaml.  Saving to ~/.mason/config.yaml
+-------------------------------------------------+
| Reading configuration at ~/.mason/config.yaml:  |
+-------------------------------------------------+
{
 "metastore_config": "{'client': 'glue', 'configuration': {'region': 'us-east-1', 'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue'}}",
 "storage_config": "{'client': 's3', 'configuration': {'region': 'us-east-1'}}",
 "scheduler_config": "{'client': 'glue', 'configuration': {'region': 'us-east-1', 'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue'}}",
 "execution_config": "{}"
}
```

You will begin without any operators registered by default:
```
mason operator
>>
No Operators Registered.  Register operators by running "mason register"

```
  You can register some example operators.  Operators are validated for basic structure using json_schema.  See `/operators/schema.json` for the schema description.
```
mason register examples/operators/table
>>
Valid Operator Definition examples/operators/table/refresh/operator.yaml
Valid Operator Definition examples/operators/table/get/operator.yaml
Valid Operator Definition examples/operators/table/list/operator.yaml
Valid Operator Definition examples/operators/table/infer/operator.yaml
Registering operator(s) at examples/operators/table to ~/.mason/operators/table/
```
Listing Operators:
```
mason operator
>>
+--------------------------------------------------+
| Available Operator Methods: ~/.mason/operators/  |
+--------------------------------------------------+

namespace    command    description                                                                               parameters
-----------  ---------  ----------------------------------------------------------------------------------------  ----------------------------------------------------------------
table        refresh    Refresh metastore tables                                                                  {'required': ['database_name', 'table_name']}
table        get        Get metastore table contents                                                              {'required': ['database_name', 'table_name']}
table        list       Get metastore tables                                                                      {'required': ['database_name']}
table        infer      Registers a schedule for infering the table then does a one time trigger of the refresh.  {'required': ['database_name', 'storage_path', 'schedule_name']}

```
Listing Operators for a particular namespace:
```
mason operator table
```

Running operator with parameters argument:
```
mason operator table get -p database_name:crawler-poc,table_name:catalog_poc_data
>>
+--------------------+
| Parsed Parameters  |
+--------------------+
{'database_name': 'crawler-poc', 'table_name': 'catalog_poc_data'}

+-------------------------+
| Parameters Validation:  |
+-------------------------+
Validated: ['database_name', 'table_name']

+--------------------+
| Operator Response  |
+--------------------+
{
 "Errors": [],
 "Info": [],
 "Warnings": [],
 "Data": {
  "name": "catalog_poc_data",
  "created_at": "2020-02-26T12:57:31-05:00",
  "created_by": "arn:aws:sts::062325279035:assumed-role/AWSGlueServiceRole-anduin-data-glue/AWS-Crawler",
  "database_name": "crawler-poc",
  "schema": [...]
 }
}

```
Running operator with config parameters yaml file:

```
mason operator table get -c examples/parameters/table_get.yaml
>>
+--------------------+
| Operator Response  |
+--------------------+
{
 "Errors": [],
 "Info": [],
 "Warnings": [],
 "Data": {
  "name": "catalog_poc_data",
  "created_at": "2020-02-26T12:57:31-05:00",
  "created_by": "arn:aws:sts::062325279035:assumed-role/AWSGlueServiceRole-anduin-data-glue/AWS-Crawler",
  "database_name": "crawler-poc",
  "schema": [...]
 }
}
```

Running flask web server for registered operator API endpoints:
```
mason run
```


## Philosophy

Mason's main function is to broker the relationship between 3 main objects:
1. Clients -  Technologies which can be used in various capacities as engines
2. Engines -  The 4 main types of data engines by default (storage, execution, metastore and scheduler) which comprise of the various ways by which a client can be utilized.
3. Operators - Parameterized definitions of work which interact with the various engines.

You can think of the interaction between these 3 types of entities as follows:  
```buildoutcfg
For <CLIENT> as a <ENGINE TYPE> do <OPERATOR DEFINITION(:parameters)>
```
For example:
```buildoutcfg
For <Glue> as a <Metastore> do <List Tables>
For <Glue> as a <Scheduler> do <Trigger schedule(:schedule_name = 'test-crawler')>
For <S3> as a <Metastore> do <List Partitions(:table_name = 'test-table')>
```

In other words Engines define the valid operations which can be performed via the various clients and operators implement one or more of those operations.  In reality operators actually define sentences like the above in a complete description of work for multiple engines, i.e. one operator can implement several such statements.

## Engines 

Out of the box mason creates a layer of abstraction between these 4 engine types (storage, metastore, scheduler, execution). This is based on the observation that most data pipelines are executing operations which interact with clients serving primarily in these 4 roles.

### Defining new Engines

COMING SOON

## Clients

Clients are being added they include a number of prioprietary technologies such as Athena, Glue, Redshift but are mainly focused on open source technologies such as Presto, Airflow and Spark.

### Defining new Clients

COMING SOON

## Operators 

The main concept in mason is something called a "Data Operator".  You can think of there as being are three main types of Data Operators:

1.  Ingress Operators
2.  Transform Operators
3.  Egress Operators

![Data Operators](images/DataOperators.png)

### Defining new Operators:
COMING SOON


### Example: Import Operator

![Ingress Operator](images/IngressOperator.png)

### Example: Dedupe Operator

![Dedupe Operator](images/DedupeOperator.png)

### Example: Summarize Operator

![Summarize Operators](images/SummarizeOperator.png)

### Example: Export Operator

![Export Operators](images/ExportOperator.png)
