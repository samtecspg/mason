from mason.util.uuid import uuid4

class SparkConfig:
    def __init__(self, config: dict):
        self.language = "scala"
        
        if self.language == "scala":
            self.app_name = "mason-spark"
            self.library_version = config.get("library_version") or "0.0.4"
            self.script_type = config.get("script_type") or "scala"
            self.spark_version = config.get("spark_version") or "3.0.0"
            self.main_class = config.get("main_class") or "mason.spark.Main"
            self.docker_image = config.get("docker_image") or f"samtecspg/mason-spark:{self.library_version}"
            self.application_file = "local://" + (config.get("application_file") or f"/opt/spark/jars/mason-spark-latest.jar")
            self.driver_cores = int(config.get("driver_cores") or 1)
            self.driver_memory_mbs = int(config.get("driver_memory_mbs") or 512)
            self.executors = int(config.get("executors") or 1)
            self.executor_memory_mb = int(config.get("executor_memory_mb") or 512)
            self.executor_cores = int(config.get("executor_cores") or 1)
        elif self.language == "python":
            self.app_name = "mason-pyspark"
            self.library_version = config.get("library_version") or "0.0.1"
            self.script_type = config.get("script_type") or "python"
            self.spark_version = config.get("spark_version") or "3.0.0"
            self.main_class = None
            self.docker_image = config.get("docker_image") or f"samtecspg/mason-pyspark:{self.library_version}"
            self.application_file = "local://" + (config.get("application_file") or "/opt/spark/mason/src/main.py")
            self.driver_cores = int(config.get("driver_cores") or 1)
            self.driver_memory_mbs = int(config.get("driver_memory_mbs") or 512)
            self.executors = int(config.get("executors") or 1)
            self.executor_memory_mb = int(config.get("executor_memory_mb") or 512)
            self.executor_cores = int(config.get("executor_cores") or 1)

    def job_name(self, job: str):
        return self.app_name + "-" + job + "-" + str(uuid4())
