
from clients.spark.runner.kubernetes_operator import merge_config
from configurations.valid_config import ValidConfig
from engines.execution.models.jobs.merge_job import MergeJob
from configurations.config import Config
from util.environment import MasonEnvironment
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

        job = MergeJob()

        merged = merge_config(config, )

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
# class TestAthenaInfer:
#
#     def test_e2e(self):
#         load_dotenv(from_root("/.env"), override=True)
#         s3_config = {}
#
#         athena_client = AthenaClient({"access_key": environ["AWS_ACCESS_KEY_ID"], "secret_key": environ["AWS_SECRET_ACCESS_KEY"], "aws_region": environ["AWS_REGION"]})
#         s3_client = S3Client(s3_config)
#
#         table, invalid = s3_client.infer_table("test_infer_table", "spg-mason-demo/part_data_merged")
#         database: Union[Database, InvalidDatabase] = athena_client.get_database("crawler-poc")
#         if table:
#             ddl = athena_client.generate_table_ddl(table, Path("s3://spg-mason-demo/athena/"))
#             job = athena_client.execute_ddl(ddl, database)
#
