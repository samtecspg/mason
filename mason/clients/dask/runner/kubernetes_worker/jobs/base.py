
from dask_kubernetes import make_pod_spec, KubeCluster
from distributed import Client, fire_and_forget
import dask
from mason_dask.jobs.format import FormatJob as DaskFormatJob


def run_job(job_type: str, spec: dict, scheduler: str, mode="async", adaptive=False):

    if job_type == "format":
        dask_job = DaskFormatJob(spec)
    else:
        raise NotImplementedError(f"Job not implemented: {job_type}")
    
    if scheduler == "local":
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

    if scheduler == "local":
        dask_job.run()
    else:
        dask.config.set({'distributed.scheduler.allowed-failures': 50})
        future = client.submit(dask_job.run)
        if mode == "sync":
            client.gather(future)
        else:
            fire_and_forget(future)
