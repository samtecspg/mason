from util.uuid import uuid4

class SparkConfig:
    def __init__(self, config: dict):
        self.app_name = "mason-spark"
        self.script_type = config.get("script_type") or "scala"
        self.spark_version = config.get("spark_version") or "2.4.5"
        self.main_class = config.get("main_class") or "mason.spark.Main"
        self.docker_image = config.get("docker_image") or f"samtecspg/mason-spark:{self.spark_version}_v1.02"
        self.application_file = "local://" + (config.get("application_file") or f"/opt/spark/jars/mason-spark-assembly-1.02.jar")
        self.driver_cores = int(config.get("driver_cores") or 1)
        self.driver_memory_mbs = int(config.get("driver_memory_mbs") or 512)
        self.executors = int(config.get("executors") or 1)
        self.executor_memory_mb = int(config.get("executor_memory_mb") or 512)
        self.executor_cores = int(config.get("executor_cores") or 1)

    def job_name(self, job: str):
        return self.app_name + "-" + job + "-" + str(uuid4())
