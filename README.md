# Mason - Data Operator Framework ![Mason Logo](/images/MasonLogo.png) 

Mason is the connecting tissue for your data projects.   It provides a "data aware" analogue to the concept of an airflow operator.   In reality it specifies operators which interact with 4 configuration abstractions:

1.   Storage Engine - for example S3, HDFS or Kafka
2.   Metadata Store - Hive, Glue, Iceberg
3.   Execution Engines -  Spark, Dask, Presto, Athena 
4.   Schedulers - Glue, Apache Airflow, DigDag

![Operator Configs](/images/OperatorConfigs.png)

Mason is heavily inspired by language agnostic configuration driven tools like kubernetes, helm, and terraform.   Mason aims to help to make existing higher level open source big data tools _easier_ to coordinate with one another and make them easier to interact with for individuals of various expertise across organizations.  Mason does not attempt to make provisioning and standing up such services referenced in its configurations easier and thus is meant to be used in conjunction with tools
like kubernetes and helm.

Mason's mission is to provide ways to build composable self contained functional units called "Data Operators" which companies can stitch together to easily provide end to end data pipelines.   The target demographic of Mason are companies that are just breaking into the enterprise data space, or companies that are looking to consolidate their data operations.

## Data Operators

![Data Operators](/images/DataOperators.png)

The main concept in mason is something called a "Data Operator".  There are three main types of Data Operators:

1.  Ingress Operators
2.  Transform Operators
3.  Egress Operators

## Usage
Local Development:
```
./pip_install
```
Configuring:
```
mason config examples/config/config_example.yaml
```
Registering Operators:
```
mason operator
```
Listing Operators:
```
mason operator
```


### Example: Import Operator

![Ingress Operator](/images/IngressOperator.png)

### Example: Dedupe Operator

![Dedupe Operator](/images/DedupeOperator.png)

### Example: Summarize Operator

![Summarize Operators](/images/SummarizeOperator.png)

### Example: Export Operator

![Export Operators](/images/ExportOperator.png)
