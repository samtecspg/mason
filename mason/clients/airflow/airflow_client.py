from botocore.client import BaseClient
from typing import Tuple, Optional

from mason.engines.scheduler.models.dags.valid_dag import ValidDag
from mason.util.environment import MasonEnvironment
from mason.clients.response import Response

class AirflowClient:

    def __init__(self, config: dict):
        self.endpoint = config.get("endpoint")
        self.user = config.get("user")
        self.password = config.get("password")

    def client(self) -> BaseClient:
        # TODO: 
        # return airflow client using endpoint, user, password 
        pass

    def register_dag(self, schedule_name: str, valid_dag: ValidDag, response: Response) -> Tuple[str, Response]:
        # TODO:  How to register DAGS to airflow using API?   Api proxy to sync? 
        response: Response = response
        
        self.dag = valid_dag
        
        return (schedule_name, response)
    
    def trigger_schedule(self, schedule_name: str, response: Response, env: MasonEnvironment) -> Response:
        
        #  TODO :  client POST /api/experimental/dags/<schedule_name>/dag_runs to airflow_response
        # airflow_response = {}
        # if airflow_response.sucess:
        #     response.add_info("Dag ${id} triggered")
        # else:
        #     response.add_error("Dag not found.  Run 'register_dag' first.")

        return response

