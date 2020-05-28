
from hiyapyco import dump as hdump

from mason.clients.spark import SparkConfig
from mason.clients.spark.runner.kubernetes_operator import merge_config
from mason.configurations.config import Config
from mason.configurations.valid_config import ValidConfig
from mason.engines.execution.models.jobs.merge_job import MergeJob
from mason.engines.storage.models.path import Path
from mason.test.support.testing_base import clean_string
from mason.util.environment import MasonEnvironment

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


