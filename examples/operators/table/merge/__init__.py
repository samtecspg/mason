from configurations import Config
from util.environment import MasonEnvironment
from parameters import Parameters
from clients.response import Response
from api import operator_api as OperatorApi

def api(*args, **kwargs): return OperatorApi.get("table", "refresh", *args, **kwargs)

def run(env: MasonEnvironment, config: Config, parameters: Parameters, response: Response):
    # print("HERE")

    ## Initial Go:
    ## Metastore = S3
    ## Execution Engine = Spark
    ## Storage Engine = S3
    ## Scheduler = None, manual for initial go

    ### Schema Merge Operator
    ### First case:
    ###  For an s3 path with parquet files.
    ###  Merge the schemas of the parquet files.
    ###  Save back out to parquet
    response.add_error("Unimplemented")

    return response


