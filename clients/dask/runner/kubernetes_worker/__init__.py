from typing import Union

from clients.dask import DaskConfig
from clients.dask.runner import DaskRunner
from definitions import from_root

from engines.execution.models.jobs import Job, InvalidJob
from engines.execution.models.jobs.infer_job import InferJob

from dask_kubernetes import KubeCluster

from dask.distributed import Client
import dask.array as da

def merge_config(config: DaskConfig, job: Job):
    base_config_file = from_root("/clients/dask/runner/kubernetes_operator/base_config.yaml")

    merge_document = {
        'metadata' : {
            'name': config.job_name(job_name)
        },
        'spec': {
            'arguments': param_list,
            'image': config.docker_image,
            'mainClass': config.main_class,
            'mainApplicationFile': config.application_file,
            'sparkVersion': config.spark_version,
            'driver': {
                'cores': config.driver_cores,
                'memory': str(config.driver_memory_mbs) + 'm',
                'labels': {'version': config.spark_version}
            },
            'executor' : {
                'cores': config.executor_cores,
                'instances': config.executors,
                'memory': str(config.executor_memory_mb) + 'm',
                'labels': {'version': config.spark_version}
            }
        }
    }

    arguments = yaml.dump(merge_document)
    conf = hload(base_config_file, arguments, method=hiyapyco.METHOD_MERGE, usedefaultyamlloader=True)
    return conf


class KubernetesWorker(DaskRunner):

    def run(self, config: DaskConfig, job: Job) -> Union[Job, InvalidJob]:

        if isinstance(job, InferJob):
            self.infer(config, job)
        else:
            InvalidJob("Job type not supported for Dask")

        return job


    def infer(self, config: DaskConfig, job: InferJob):
        job.database
        job.path

        cluster = KubeCluster.from_yaml('worker-spec.yml')
        cluster.scale(10)  # specify number of workers explicitly

        cluster.adapt(minimum=1, maximum=100)  # or dynamically scale based on current workload

        # Connect Dask to the cluster
        client = Client(cluster)

        # Create a large array and calculate the mean
        array = da.ones((1000, 1000, 1000))
        print(array.mean().compute())  # Should print 1.0



