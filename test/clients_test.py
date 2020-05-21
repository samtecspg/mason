import os

import pytest
from dotenv import load_dotenv

from clients.aws.athena import AthenaClient
from clients.aws.glue import GlueClient
from clients.aws.s3 import S3Client
from clients.dask import DaskClient
from clients.response import Response

from clients.spark.runner.kubernetes_operator import merge_config
from configurations.valid_config import ValidConfig
from definitions import from_root
from engines.execution.models.jobs.infer_job import InferJob
from engines.metastore.models.credentials import MetastoreCredentials
from configurations.config import Config
from engines.metastore.models.database import Database
from test.support.testing_base import clean_uuid, clean_string
from util.environment import MasonEnvironment
from hiyapyco import dump as hdump
from clients.spark import SparkConfig

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

        parameters = {
            "job": "merge",
            "test-parameter": "test-value",
            "test-parameter-2": "test-value-2"
        }

        mc = MetastoreCredentials()

        merged = merge_config(config, "test_job", mc, parameters)
        dumped = hdump(merged)
        expects = """
            apiVersion: sparkoperator.k8s.io/v1beta2
            kind: SparkApplication
            metadata:
              name: mason-spark-test_job-
              namespace: default
            spec:
              arguments:
              - --job
              - test_job
              - --test-parameter
              - test-value
              - --test-parameter-2
              - test-value-2
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
        assert clean_string(clean_uuid(dumped)) == clean_string(clean_uuid(expects))


class TestS3:

    def test_parse_path(self):
        env = MasonEnvironment()
        conf = Config({"metastore_engine": "s3", "clients": {"s3": {"configuration": {"aws_region": "test", "secret_key": "test", "access_key": "test"}}}}).validate(env)
        if isinstance(conf, ValidConfig):
            assert(conf.metastore.client.__class__.__name__ == "S3MetastoreClient")
            client = conf.metastore.client
            parsed = client.parse_path("test_bucket/test_path/test_file.csv")
            assert(parsed[0] == "test_bucket")
            assert(parsed[1] == "test_path/test_file.csv")


# @pytest.mark.skip(reason="This is not mocked, hits live endpoints")
class TestDask:

    def test_e2e(self):
        load_dotenv(from_root("/.env"), override=True)
        dask_config = {"runner": {"type": "kubernetes_worker", "scheduler": "dask-scheduler:8786"}}
        s3_config = {}
        glue_config = {}

        database_name = "crawler_poc"
        path_name = "test_path"

        dask_client = DaskClient(dask_config)
        glue_client = GlueClient(glue_config)
        s3_client = S3Client(s3_config)

        database = glue_client.get_database(database_name)
        if isinstance(database, Database):
            path = s3_client.get_path(path_name)
            job = InferJob(database, path)

            response = dask_client.run_job(job, Response())
            print(response.formatted())
        else:
            print(database.reason)

