from typing import Union
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob

def run_job(job_type: str, spec: dict, scheduler: str, mode="async", adaptive=False) -> Union[ExecutedJob, InvalidJob]:
    
    # from dask_kubernetes import KubeCluster, make_pod_spec

    from distributed import Client, fire_and_forget
    import dask

    from mason_dask.jobs.executed import ExecutedJob as ExecutedDaskJob
    from mason_dask.jobs.executed import InvalidJob as InvalidDaskJob

    from mason_dask.jobs.format import FormatJob as DaskFormatJob
    from mason_dask.utils.cluster_spec import ClusterSpec

    if job_type == "format":
        job = DaskFormatJob(spec)
    else:
        raise NotImplementedError(f"Job not implemented: {job_type}")
    
    dask_job = job.validate()
    
    def get_client():
        if scheduler.startswith("local"):
            return Client()
        else:
            # if adaptive == True:
            #     spl = scheduler.split(":")
            #     host = spl[0]
            #     port = spl[1]
            # 
            #     pod_spec = make_pod_spec(
            #         image='daskdev/dask:latest',
            #         env={'EXTRA_PIP_PACKAGES': 'git+https://github.com/dask/distributed s3fs pyexcelerate --upgrade',
            #              'EXTRA_CONDA_PACKAGES': 'fastparquet -c conda-forge'}
            #     )
            #     cluster = KubeCluster(pod_spec, deploy_mode="local")
            #     cluster.port = port
            #     cluster.host = host
            #     # cluster.adapt(minimum=0, maximum=100)
            #     return Client(cluster)
            # else:
            return Client(scheduler)

    def to_mason_job(job: Union[ExecutedDaskJob, InvalidDaskJob]):
        if isinstance(job, ExecutedDaskJob):
            return ExecutedJob("format-job", job.message)
        else:
            return InvalidJob(job.message)
    
    with get_client() as client:
        cluster_spec = ClusterSpec(client)
        
        mode = "sync"
        final: Union[ExecutedJob, InvalidJob]
        if isinstance(dask_job, InvalidDaskJob):
            final = InvalidJob(f"Invalid Dask Job: {dask_job.message}")
        else:
            if scheduler.startswith("local"):
                result: Union[ExecutedDaskJob, InvalidDaskJob] = dask_job.run(cluster_spec)
                final = to_mason_job(result)
            else:
                dask.config.set({'distributed.scheduler.allowed-failures': 50})
                future = client.submit(dask_job.run, cluster_spec)
                if mode == "sync":
                    result: Union[ExecutedDaskJob, InvalidDaskJob] = client.gather(future)
                    final = to_mason_job(result)
                else:
                    fire_and_forget(future)
                    final = ExecutedJob(f"Queued job {dask_job} to run against dask scheduler: {scheduler}")
                    
    return final