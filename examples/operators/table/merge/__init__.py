from configurations import Config
from util.environment import MasonEnvironment
from parameters import Parameters
from clients.response import Response
from api import operator_api as OperatorApi
from util.logger import logger

def api(*args, **kwargs): return OperatorApi.get("table", "merge", *args, **kwargs)

def run(env: MasonEnvironment, config: Config, parameters: Parameters, response: Response):
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

    response = Response()
    output_path: str = parameters.safe_get("output_path")
    response = config.execution.client.run_job("schema_merge", response)

    return response


