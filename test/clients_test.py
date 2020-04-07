
from clients.spark.runner.kubernetes_operator import merge_config
from definitions import from_root
from hiyapyco import dump as hdump # type: ignore
from clients.spark import SparkConfig


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

        base_config_file = from_root("/clients/spark/runner/kubernetes_operator/base_config.yaml")
        parameters = {
            "test-parameter": "test-value",
            "test-parameter-2": "test-value-2"
        }

        merged = merge_config(config, "test_job", parameters, base_config_file)
        dumped = hdump(merged)
        print(dumped)



