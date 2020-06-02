
from hiyapyco import dump as hdump

from mason.clients.spark.spark_client import SparkConfig
from mason.clients.spark.runner.kubernetes_operator.kubernetes_operator import merge_config
from mason.engines.execution.models.jobs.merge_job import MergeJob
from mason.engines.storage.models.path import Path
from mason.test.support.testing_base import clean_string

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




