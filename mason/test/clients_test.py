import os
import shutil
from unittest import mock

from hiyapyco import dump as hdump

from mason.clients.response import Response

from mason.test.support.mocks import mock_execution_engine_client, mock_storage_engine_client, mock_metastore_engine_client, mock_config_schema
from mason.util.logger import logger
from mason.workflows.valid_workflow import ValidWorkflow
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.definitions import from_root
from mason.configurations.configurations import get_all

from mason.clients.spark.spark_client import SparkConfig
from mason.clients.spark.runner.kubernetes_operator.kubernetes_operator import merge_config
from mason.engines.execution.models.jobs.merge_job import MergeJob
from mason.engines.storage.models.path import Path
from mason.test.support.testing_base import clean_string, clean_uuid
from mason.workflows import workflows
from mason.test.support import testing_base as base


class TestSpark:

    def test_kubernetes_operator(self):

        config = SparkConfig({
            'script_type': 'scala-test',
            'spark_version': 'test.spark.version',
            'main_class': 'test.main.Class',
            'docker_image': 'docker/test-docker-image',
            'application_file': 'test/jar/file/location/assembly.jar',
            'driver_cores': 10,
            'driver_memory_mbs': 1024,
            'executors': 3,
            'executor_memory_mb': 1024,
            'executor_cores': 20
        })

        job = MergeJob(Path("test-input"), Path("test-output"), "parquet")
        job.set_id("mason-spark-test_job")

        merged = merge_config(config, job)

        dumped = hdump(merged)
        expects = """
            apiVersion: sparkoperator.k8s.io/v1beta2
            kind: SparkApplication
            metadata:
              name: mason-spark-test_job
              namespace: default
            spec:
              arguments:
              - --input_path
              - test-input 
              - --output_path
              - test-output 
              - --input_format
              - parquet
              - --job
              - merge 
              driver:
                coreLimit: 1200m
                cores: 10
                labels:
                  version: test.spark.version
                memory: 1024m
                serviceAccount: spark
                volumeMounts:
                - mountPath: /tmp
                  name: test-volume
              executor:
                cores: 20
                instances: 3
                labels:
                  version: test.spark.version
                memory: 1024m
                volumeMounts:
                - mountPath: /tmp
                  name: test-volume
              image: docker/test-docker-image
              imagePullPolicy: Always
              mainApplicationFile: local://test/jar/file/location/assembly.jar
              mainClass: test.main.Class
              mode: cluster
              restartPolicy:
                type: Never
              sparkVersion: test.spark.version
              type: Scala
              volumes:
              - hostPath:
                  path: /tmp
                  type: Directory
                name: test-volume
        """
        assert clean_string(dumped) == clean_string(expects)



@mock.patch("mason.engines.execution.execution_engine.ExecutionEngine.get_client", mock_execution_engine_client)
@mock.patch("mason.engines.storage.storage_engine.StorageEngine.get_client", mock_storage_engine_client)
@mock.patch("mason.engines.metastore.metastore_engine.MetastoreEngine.get_client", mock_metastore_engine_client)
@mock.patch("mason.configurations.config.Config.config_schema", mock_config_schema)
class TestLocal:

    def before(self):
        base.set_log_level("fatal")
        mason_home = from_root("/test/.tmp/")

        if os.path.exists(mason_home):
            shutil.rmtree(mason_home)

        env = base.get_env(workflow_home="/test/.tmp/workflows/", operator_home="/test/support/operators/",
                           config_home="/test/support/configs/", operator_module="mason.test.support.operators", workflow_module="mason.test.support.workflows")

        return env, mason_home

    def test_local_client(self):
        env, mason_home = self.before()
        config = get_all(env)[0]['8']

        # DAG has cycle
        step_params = {
            "config_id": "8",
            "parameters": {
                "test_param": "test"
            }
        }
        params = {
            "step_1": step_params,
            "step_2": step_params,
            "step_3": step_params,
            "step_4": step_params,
            "step_5": step_params,
            "step_6": step_params,
        }

        workflows.register_workflows(from_root("/test/support/workflows/testing_namespace/workflow_local_scheduler/"), env)
        wf = workflows.get_workflow(env, "testing_namespace", "workflow_local_scheduler")
        logger.set_level("fatal")
        if wf:
            parameters = WorkflowParameters(parameter_dict=params)
            validated = wf.validate(env, config, parameters)
            assert(isinstance(validated, ValidWorkflow))
            response = validated.execute(env, Response(), False, True)
            info = """
            Registering workflow dag test_workflow_local_scheduler_ea5b602c-261c-4e06-af21-375ea912b6a5 with local.
            Registering DAG in local memory
            Registered schedule test_workflow_local_scheduler_ea5b602c-261c-4e06-af21-375ea912b6a5
            Triggering schedule: test_workflow_local_scheduler_ea5b602c-261c-4e06-af21-375ea912b6a5
            Running dag
            * step_1
            | * step_2
            | | * step_3
            | |/  
            * | step_4
            |/  
            * step_5
            * step_6

            Running step step_1
            Running operator1
            Running step step_2
            Running operator2
            Running step step_3
            Running operator3
            Running step step_4
            Running operator4
            Running step step_5
            Running operator5
            Running step step_6
            Running operator6
            """
            assert(len(response.errors)  == 0)
            assert(clean_uuid(clean_string("\n".join(response.info))) == clean_uuid(clean_string(info)))
        else:
            raise Exception("Workflow not found")




