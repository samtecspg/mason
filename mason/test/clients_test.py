from hiyapyco import dump as hdump

from mason.clients.airflow.airflow_client import AirflowClient
from mason.clients.airflow.scheduler import AirflowSchedulerClient
from mason.clients.athena.athena_client import AthenaClient
from mason.clients.athena.execution import AthenaExecutionClient
from mason.clients.athena.metastore import AthenaMetastoreClient
from mason.clients.dask.dask_client import DaskClient
from mason.clients.dask.execution import DaskExecutionClient
from mason.clients.glue.glue_client import GlueClient
from mason.clients.glue.metastore import GlueMetastoreClient
from mason.clients.glue.scheduler import GlueSchedulerClient
from mason.clients.local.execution import LocalExecutionClient
from mason.clients.local.metastore import LocalMetastoreClient
from mason.clients.local.scheduler import LocalSchedulerClient
from mason.clients.response import Response
from mason.clients.s3.metastore import S3MetastoreClient
from mason.clients.s3.s3_client import S3Client
from mason.clients.s3.storage import S3StorageClient
from mason.clients.spark.execution import SparkExecutionClient
from mason.clients.local.local_client import LocalClient
from mason.resources.malformed import MalformedResource
from mason.resources.base import Resources

from mason.util.environment import MasonEnvironment
from mason.workflows.valid_workflow import ValidWorkflow
from mason.parameters.workflow_parameters import WorkflowParameters

from mason.clients.spark.spark_client import SparkConfig, SparkClient
from mason.clients.spark.runner.kubernetes_operator.kubernetes_operator import merge_config
from mason.engines.execution.models.jobs.merge_job import MergeJob
from mason.engines.storage.models.path import Path
from mason.test.support.testing_base import clean_string, clean_uuid
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

class TestLocal:

    def before(self):
        return base.get_env("/test/support/", "/test/support/validations/")
    
    def get_workflow(self, env: MasonEnvironment, command: str):
        return Resources(env).get_workflow("testing_namespace", command)

    def test_local_client(self):
        base.set_log_level()
        env = self.before()
        config = Resources(env).get_config("8") 

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
        
        wf = self.get_workflow(env, "workflow_local_scheduler")

        if isinstance(wf, MalformedResource):
            raise Exception(f"Workflow not found: {wf.get_message()}")

        if isinstance(config, MalformedResource):
            raise Exception(f"Config not found: {config.get_message()}")

        parameters = WorkflowParameters(parameter_dict=params)
        validated = wf.validate(env, config, parameters)
        assert(isinstance(validated, ValidWorkflow))
        operator_response = validated.run(env, Response())
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
        response = operator_response.response
        assert(len(response.errors)  == 0)
        assert(clean_uuid(clean_string("\n".join(response.info))) == clean_uuid(clean_string(info)))


class TestOthers:
    
    #  These tests ensure metatore clients are explicitely instantiated to ensure that they are hit at least once for mypy
    def test_s3_client(self):
        s3_client = S3Client()
        s3_metastore = S3MetastoreClient(s3_client)
        s3_storage = S3StorageClient(s3_client)

    def test_spark_client(self):
        spark_client = SparkClient({"type": "operator"})
        spark_execution = SparkExecutionClient(spark_client)
        
    def test_airflow_client(self):
        airflow_client = AirflowClient("endpoint", "user", "password")
        airflow_scheduler = AirflowSchedulerClient(airflow_client)
    
    def test_athena_client(self):
        athena_client = AthenaClient()
        athena_metastore_client = AthenaMetastoreClient(athena_client)
        athena_execution_client = AthenaExecutionClient(athena_client)

    def test_dask_client(self):
        dask_client = DaskClient({})
        dask_execution_client = DaskExecutionClient(dask_client)
    
    def test_glue_client(self):
        glue_client = GlueClient()
        glue_metastore_client = GlueMetastoreClient(glue_client)
        glue_scheduler_client = GlueSchedulerClient(glue_client)
    
    def test_local_client(self):
        local_client = LocalClient()
        local_execution_client = LocalExecutionClient(local_client)
        local_scheduler_client = LocalSchedulerClient(local_client)
        local_metastore_client = LocalMetastoreClient(local_client)



