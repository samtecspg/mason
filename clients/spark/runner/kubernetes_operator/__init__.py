from uuid import uuid4

from clients.response import Response
from clients.spark.runner import SparkRunner
from clients.spark.config import SparkConfig
from definitions import from_root
from engines.execution.models.jobs import Job, ExecutedJob, InvalidJob
from util.sys_call import run_sys_call
from hiyapyco import load as hload
from typing import List, Union
import yaml
import tempfile

import hiyapyco

def prep_parameters(params: dict) -> List[str]:
    param_list: List[str] = []
    for k,v in params.items():
        param_list.append("--" + k)
        param_list.append(v)
    return param_list

def merge_config(config: SparkConfig, job: Job):
    base_config_file = from_root("/clients/spark/runner/kubernetes_operator/base_config.yaml")

    parameters = job.parameters or {}
    parameters["job"] = job.type
    param_list = prep_parameters(parameters)

    merge_document = {
        'metadata' : {
            'name': job.id
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

class KubernetesOperator(SparkRunner):

    def run(self, config: SparkConfig, job: Job) -> Union[ExecutedJob, InvalidJob]:
        #  TODO: Replace with python kubernetes api
        #  TODO: Set up kubernetes configuration, run on docker version

        job.set_id(job.type + "_" + str(uuid4()))
        merged_config = merge_config(config, job)
        job_id = merged_config["metadata"]["name"]
        conf = dict(merged_config)

        with tempfile.NamedTemporaryFile(delete=False, mode='w') as yaml_file:
            yaml_dump = yaml.dump(conf, yaml_file)

            command = ["kubectl", "apply", "-f", yaml_file.name]
            job.add_log(f"Executing Spark Kubernetes Operator. job_id:  {job_id}")
            stdout, stderr = run_sys_call(command)

            if len(stdout) > 0:
                job.add_log(stdout)
                return job.running()
            else:
                if len(stderr) > 0:
                    job.add_log(stderr)
                    return job.errored(stderr)
                else:
                    return job.running()

    def get(self, job_id: str, response: Response) -> ExecutedJob:
        job = Job("spark", response=response)
        command = ["kubectl", "logs", job_id + "-driver"]
        stdout, stderr = run_sys_call(command)

        if len(stdout) > 0:
            job.response.add_data({'Logs': stdout})
        if len(stderr) > 0:
            job.response.add_error(stderr)

        return ExecutedJob(job)

