
from typing import Union
import dask
from distributed import Client, fire_and_forget

def run(spec: dict, scheduler: str):
    
    class CompleteDaskJob:
        def __init__(self, message: str = ""):
            self.message = message

    class InvalidDaskJob():
        def __init__(self, message: str = ""):
            self.message = message

    class DaskQueryJob():
        
        def __init__(self, job_spec: dict):
            self.query_string = job_spec.get("query_string")
            self.database = job_spec.get("database")
            self.output_path = job_spec.get("output_path")

        def run_job(self) -> Union[CompleteDaskJob, InvalidDaskJob]:
            # df: DataFrame = dd.read_sql_table(self.query_string)
            if self.output_path:
                # df.to_parquet(self.output_path)
                return CompleteDaskJob(f"Job to query via Dask succesfully queued to scheduler")
            else:
                return InvalidDaskJob("Output path required for Dask implementation of table query")

    dask_job = DaskQueryJob(spec)
    mode = "async"

    if scheduler == "local":
        client = Client()
        dask_job.run_job()
    else:
        dask.config.set({'distributed.scheduler.allowed-failures': 50})
        client = Client(scheduler)
        future = client.submit(dask_job.run_job)
        if mode == "sync":
            client.gather(future)
        else:
            fire_and_forget(future)

