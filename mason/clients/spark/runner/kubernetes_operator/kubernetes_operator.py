from hiyapyco import load as hload
from typing import List, Union, Tuple, Optional, Dict, Any
import yaml
import tempfile
import hiyapyco
from kubernetes.client import ApiException

from mason.clients.response import Response
from mason.clients.spark.config import SparkConfig
from mason.clients.spark.runner.spark_runner import SparkRunner
from mason.engines.execution.models.jobs import Job, ExecutedJob, InvalidJob
from mason.util.exception import message
from mason.util.uuid import uuid4
from mason.definitions import from_root
from mason.util.sys_call import run_sys_call

def prep_parameters(params: dict) -> List[str]:
    param_list: List[str] = []
    for k,v in params.items():
        param_list.append("--" + k)
        if isinstance(v, bool):
            if (v == True):
                param_list.append("true")
            else:
                param_list.append("false")
        else:
            # TODO: find a better way here
            if isinstance(v, str):
                param_list.append(v.replace("s3://", "s3a://"))
            else:
                param_list.append(str(v))
            
    return param_list

def merge_config(config: SparkConfig, job: Job):
    base_config_file = from_root(f"/clients/spark/runner/kubernetes_operator/base_config_{config.language}.yaml")
    parameters = job.parameters or {}
    parameters["job"] = job.type
    param_list = prep_parameters(parameters)

    merge_document: Dict[str, Any] = {
        'metadata': {
            'name': job.id
        }
    }

    merge_document['spec'] = {
        'arguments': param_list,
        'image': config.docker_image,
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

    if config.main_class:
        merge_document['mainClass'] = config.main_class

    arguments = yaml.dump(merge_document)
    conf = hload(base_config_file, arguments, method=hiyapyco.METHOD_MERGE, usedefaultyamlloader=True)
    return conf

class KubernetesOperator(SparkRunner):

    def run(self, config: SparkConfig, job: Job, resp: Response = Response()) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        #  TODO: Replace with python kubernetes api
        #  TODO: Set up kubernetes configuration, run on docker version
        response: Response = resp or Response()
        
        job.set_id("mason" + "-" + job.type + "-" + str(uuid4()))
        merged_config = merge_config(config, job)
        job_id = merged_config["metadata"]["name"]
        conf = dict(merged_config)
        final: Union[ExecutedJob, InvalidJob]

        try:
            from kubernetes import client, config as k8s_config
            k8s_config.load_kube_config()
            api = client.CustomObjectsApi()
            api_response = api.create_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace="default",
                plural="sparkapplications",
                body=conf,
            )
            response.add_response(api_response)
            try:
                link = (api_response.get("metadata") or {}).get("selfLink")
                final = job.running(f"Scheduled job with spark on k8s operator: {link}")
            except Exception as e:
                final = job.running("Scheduled job with spark on k8s operator")
        except ApiException as e:
            response.set_status(e.status)
            final = InvalidJob(f"Error while submitting job to kubernetes: {message(e)}")
        except Exception as e:
            final = InvalidJob(f"Error while submitting job to kubernetes: {message(e)}")
            
        return final, response

    def get(self, job_id: str, resp: Response = Response()) -> Tuple[ExecutedJob, Response]:
        response: Response = resp or Response()
        
        command = ["kubectl", "logs", job_id + "-driver"]
        response.add_info(f"Running {command}")
        
        stdout, stderr = run_sys_call(command)
        final: Union[ExecutedJob, InvalidJob]
        all = stdout + stderr
        for err in stderr:
            if "NotFound" in err:
                response.set_status(400)
                
        final = ExecutedJob(job_id, logs=all)

        return final, response

