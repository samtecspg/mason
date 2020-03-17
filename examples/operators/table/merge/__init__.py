from configurations import Config
from parameters import Parameters
from clients.response import Response
from api import operator_api as OperatorApi

def api(*args, **kwargs): return OperatorApi.get("table", "refresh", *args, **kwargs)

def run(config: Config, parameters: Parameters, response: Response):
    # print("HERE")

    ## Metastore = Parquet
    ## Storage Engine = S3
    ## Execution Engine = Spark
    ## Scheduler = Airflow

    ### Schema Merge Operator
    ### First case:
    ###  For an s3 path with parquet files.
    ###  Merge the schemas of the parquet files.
    ###  Save back out to parquet
    response.add_error("Unimplemented")

    return response





    # database_name: str = parameters.safe_get("database_name")
    # table_name: str = parameters.safe_get("table_name")

    # # TODO: break this up into 2 calls, remove trigger_schedule_for_table from engine definition
    # response = config.scheduler.client.trigger_schedule_for_table(table_name, database_name, response)
    #
    # return response


