
from clients.spark.runner import SparkRunner
from clients.spark.config import SparkConfig
from clients.response import Response
from util.logger import logger
from definitions import from_root
from util.sys_call import run_sys_call
from hiyapyco import load as hload # type: ignore
from typing import List
import yaml

import hiyapyco

def prep_parameters(params: dict) -> List[str]:
    param_list: List[str] = []
    for k,v in params.items():
        param_list.append("--" + k)
        param_list.append(v)
    return param_list

def merge_config(config: SparkConfig, job_name: str, parameters: dict, base_config: str):
    base_config_file = from_root("/clients/spark/runner/kubernetes_operator/base_config.yaml")
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
    conf = hload(base_config_file, arguments, method=hiyapyco.METHOD_MERGE)
    return conf

class KubernetesOperator(SparkRunner):

    def run(self, config: SparkConfig, params: dict, response: Response):
        #  TODO: Replace with python kubernetes api
        #  TODO: Set up kubernetes configuration, run on docker version

        conf = hiyapyco.load('base_config.yaml')

        command0 = ["kubectl", "delete", "sparkapplication", "mason-spark"]
        command = ["kubectl", "apply", "-f", from_root("/clients/spark/runner/kubernetes_operator/base_config.yaml")]

        logger.info("Executing Spark Kubernetes Operator")

        stdout, stderr = run_sys_call(command0, response)
        stdout, stderr = run_sys_call(command, response)

    def get(self, config: SparkConfig, response: Response):

        command1 = ["kubectl", "describe", "sparkapplication", "mason-spark"]
        command2 = ["kubectl", "logs", "mason-spark-driver"]




