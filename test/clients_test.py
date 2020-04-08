
from clients.spark.runner.kubernetes_operator import merge_config
from definitions import from_root
from hiyapyco import dump as hdump # type: ignore
from clients.spark import SparkConfig
from engines.metastore.models.credentials import MetastoreCredentials
from test.support.testing_base import assert_multiline, clean_uuid


class TestClients:

    def test_spark_kubernetes_operator(self):

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
        assert_multiline(clean_uuid(dumped), clean_uuid(expects))



