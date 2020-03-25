
class SparkClient:
    def __init__(self, spark_config: dict):
        self.threads = spark_config.get("threads")
        # self.client = ###  Depends on job scheduler, k8s or EMR, etc


