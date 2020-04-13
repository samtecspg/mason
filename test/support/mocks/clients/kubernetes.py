from clients.response import Response
from engines.metastore.models.credentials import MetastoreCredentials

from util.logger import logger
from clients.spark import SparkConfig

class KubernetesMock:

    def run(self, config: SparkConfig, job_name: str, metastore_credentials: MetastoreCredentials, params: dict, response: Response):
        if params["input_path"] == "s3a://good_input_path" and params["output_path"] == "s3a://good_output_path":
            stdout = 'sparkapplication.sparkoperator.k8s.io/mason-spark-merge- created'
            response.add_response({"STDOUT": stdout})
            response.add_info(stdout)
        else:
            raise Exception("Mock parameters not implemented for spark kubernetes implementation")



    def get(self, job_id: str, response: Response):
        if job_id == "good_job_id":
            message = {'Errors': [], 'Info': [{"Logs": "<LOG_DATA>"}], 'Warnings': []}
            response.add_info(message)
        elif job_id == "bad_job_id":
            response.add_error("Error from server (NotFound): pods \"bad_job_id-driver\" not found")
            response.set_status(500)
        else:
            raise Exception("Mock parameters not implemented for spark kubernetes implementation")





