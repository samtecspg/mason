from typing import Optional, Tuple

from distributed import Client
from returns.result import Result

from mason.engines.execution.models.jobs.query_job import QueryJob
from mason.clients.dask.runner.dask_runner import DaskRunner
from mason.clients.response import Response
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob, Job
from mason.engines.execution.models.jobs.format_job import FormatJob
from mason.util.exception import message

from typing import Union

from mason.util.result import compute

class KubernetesWorker(DaskRunner):

    def __init__(self, config: dict):
        self.scheduler: str = config.get("scheduler") or "local:8786"
        self.num_workers = config.get("num_workers")
        self.credentials = config.get("credentials")
        
    def client(self):
        if self.scheduler.startswith("local"):
            return Client()
        else:
            # from dask_kubernetes import KubeCluster, make_pod_spec
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
            return Client(self.scheduler)

    def run(self, job: Job, resp: Optional[Response] = None, mode: str = "async") -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        final: Union[ExecutedJob, InvalidJob]
        response: Response = resp or Response()
        
        try:
            if self.scheduler:
                if isinstance(job, FormatJob):
                    final = self.run_job(job, self.scheduler, mode) or ExecutedJob("format_job", f"Job queued to format {job.table.schema.type} table as {job.format} and save to {job.output_path.path_str}")
                elif isinstance(job, QueryJob):
                    final = self.run_job(job, self.scheduler)
                else:
                    final = job.errored("Job type not supported for Dask")
            else:
                final = InvalidJob("Dask Scheduler not defined")
        except OSError as e:
            final = InvalidJob(message(e))

        return final, response


    def run_job(self, job: Job, scheduler: str, mode: str = "async") -> Union[ExecutedJob, InvalidJob]:

        from distributed import fire_and_forget
        import dask

        from mason_dask.jobs.executed import ExecutedJob as ExecutedDaskJob
        from mason_dask.jobs.executed import InvalidJob as InvalidDaskJob

        from mason_dask.utils.cluster_spec import ClusterSpec

        if isinstance(job, FormatJob):
            from mason_dask.jobs.format import FormatJob as DaskFormatJob
            job = DaskFormatJob(job.spec())
        elif isinstance(job, QueryJob):
            from mason_dask.jobs.query import QueryJob as DaskQueryJob
            job = DaskQueryJob(job.spec())
        else:
            raise NotImplementedError(f"Job not implemented: {job.type}")

        # TODO: fix this in mason - dask
        dask_job = job.validate() # type: ignore  

        def to_mason_job(job: Result[ExecutedDaskJob, InvalidDaskJob]):
            j = compute(job)
            if isinstance(j, ExecutedDaskJob):
                return ExecutedJob("format-job", j.message)
            else:
                value = job._inner_value
                assert(isinstance(value, InvalidDaskJob))
                return InvalidJob(value.message)

        with self.client() as client:
            cluster_spec = ClusterSpec(client, scheduler=self.scheduler)

            final: Union[ExecutedJob, InvalidJob]
            if isinstance(dask_job, InvalidDaskJob):
                final = InvalidJob(f"Invalid Dask Job: {dask_job.message}")
            else:
                result: Result[ExecutedDaskJob, InvalidDaskJob]
                if scheduler.startswith("local"):
                    result = dask_job.run(cluster_spec)
                    final = to_mason_job(result)
                else:
                    dask.config.set({'distributed.scheduler.allowed-failures': 50})
                    future = client.submit(dask_job.run, cluster_spec)
                    if mode == "sync":
                        result = client.gather(future)
                        final = to_mason_job(result)
                    else:
                        fire_and_forget(future)
                        final = ExecutedJob(f"Queued job {dask_job} to run against dask scheduler: {scheduler}")

        return final

