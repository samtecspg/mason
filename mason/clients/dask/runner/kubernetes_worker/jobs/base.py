from typing import Optional, Union

from dask_kubernetes import make_pod_spec, KubeCluster
from distributed import Client, fire_and_forget
import dask

from mason_dask.jobs.executed import ExecutedJob as DaskExecutedJob
from mason_dask.jobs.executed import InvalidJob as DaskInvalidJob
from mason_dask.jobs.format import FormatJob as DaskFormatJob
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob

def run_job(job_type: str, spec: dict, scheduler: str, mode="async", adaptive=False) -> Optional[Union[ExecutedJob, InvalidJob]]:

    if job_type == "format":
        dask_job = DaskFormatJob(spec)
    else:
        raise NotImplementedError(f"Job not implemented: {job_type}")
    
    if scheduler.startswith("local"):
        client = Client()
    else:
        if adaptive == True:
            spl = scheduler.split(":")
            host = spl[0]
            port = spl[1]

            pod_spec = make_pod_spec(
                image='daskdev/dask:latest',
                env={'EXTRA_PIP_PACKAGES': 'git+https://github.com/dask/distributed s3fs pyexcelerate --upgrade',
                     'EXTRA_CONDA_PACKAGES': 'fastparquet -c conda-forge'}
            )
            cluster = KubeCluster(pod_spec)
            cluster.port = port
            cluster.host = host
            cluster.adapt(minimum=0, maximum=100)
            client = Client(cluster)
        else:
            client = Client(scheduler)

    final = None
    if scheduler.startswith("local"):
        result = dask_job.run(client)
        if isinstance(result, DaskExecutedJob):
            final = ExecutedJob(job_type, result.message)
        elif isinstance(result, DaskInvalidJob):
            final = InvalidJob(result.message)
    else:
        dask.config.set({'distributed.scheduler.allowed-failures': 50})
        future = client.submit(dask_job.run, client)
        if mode == "sync":
            client.gather(future)
        else:
            fire_and_forget(future)
            
    return final