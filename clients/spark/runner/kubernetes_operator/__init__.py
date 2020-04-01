
from clients.spark.runner import SparkRunner
from clients.spark.config import SparkConfig
from clients.response import Response
from util.logger import logger
from definitions import from_root
from util.sys_call import run_sys_call

class KubernetesOperator(SparkRunner):

    def run(self, config: SparkConfig, response: Response):
        #  TODO: Replace with python kubernetes api
        #  TODO: Set up kubernetes configuration, run on docker version
        command0 = ["kubectl", "delete", "sparkapplication", "spark-pi"]
        command1 = ["kubectl", "describe", "sparkapplication", "spark-pi"]
        command2 = ["kubectl", "describe", "sparkapplications"]
        command = ["kubectl", "apply", "-f", from_root("/clients/spark/runner/kubernetes_operator/config.yaml")]

        logger.info("Executing Spark Kubernetes Operator")

        stdout, stderr = run_sys_call(command, response)





