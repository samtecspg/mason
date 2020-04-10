
from clients.spark.runner import SparkRunner
from clients.spark.config import SparkConfig
from clients.response import Response
from util.logger import logger
from definitions import from_root
from util.sys_call import run_sys_call
from hiyapyco import load as hload # type: ignore
from typing import List
import yaml
from engines.metastore.models.credentials import MetastoreCredentials
import tempfile

import hiyapyco

def prep_parameters(params: dict) -> List[str]:
    param_list: List[str] = []
    for k,v in params.items():
        param_list.append("--" + k)
        param_list.append(v)
    return param_list

def merge_config(config: SparkConfig, job_name: str, metastore_credentials: MetastoreCredentials, parameters: dict):
    base_config_file = from_root("/clients/spark/runner/kubernetes_operator/base_config.yaml")

    parameters["job"] = job_name
    
    if metastore_credentials.type == "aws":
        parameters["access_key"] = metastore_credentials.access_key
        parameters["secret_key"] = metastore_credentials.secret_key

    param_list = prep_parameters(parameters)
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

class KubernetesOperator(SparkRunner):

    def run(self, config: SparkConfig, job_name: str, metastore_credentials: MetastoreCredentials, params: dict, response: Response):
        #  TODO: Replace with python kubernetes api
        #  TODO: Set up kubernetes configuration, run on docker version

        merged_config = merge_config(config, job_name, metastore_credentials, params)
        job_id = merged_config["metadata"]["name"]
        conf = dict(merged_config)

        with tempfile.NamedTemporaryFile(delete=False, mode='w') as yaml_file:
            yaml_dump = yaml.dump(conf, yaml_file)

            command = ["kubectl", "apply", "-f", yaml_file.name]

            message = f"Executing Spark Kubernetes Operator. job_id:  {job_id}"
            logger.info(message)
            response.add_info(message)

            stdout, stderr = run_sys_call(command)
            response.add_response({"STDOUT": stdout})
            response.add_response({"STDERR": stderr})
            return response

    def get(self, job_id: str, response: Response):
        command = ["kubectl", "logs", job_id + "-driver"]

        stdout, stderr = run_sys_call(command)
        response.add_response({"STDERR": stderr})
        if stdout == ['']:
            response.add_warning("Blank response from kubectl, kubectl error handling is not good for this case, its possible the job_id is incorrect.  Check job_id")
        if len(stdout) > 0:
            response.add_info({"Logs": "\n".join(stdout[-100:])})
        if len(stderr) > 0:
            response.add_error(stderr)




