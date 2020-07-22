from botocore.client import BaseClient
from typing import Tuple, Optional

from mason.engines.scheduler.models.dags.dag import Dag
from mason.engines.scheduler.models.dags.valid_dag import ValidDag
from mason.util.environment import MasonEnvironment
from mason.clients.response import Response
from mason.workflows.workflow_run import WorkflowRun


class AirflowClient:

    def __init__(self, config: dict):
        self.endpoint = config.get("endpoint")
        self.user = config.get("user")
        self.password = config.get("password")

    def client(self) -> BaseClient:
        # TODO:
        # return airflow client using endpoint, user, password 

    def register_dag(self, schedule_name: str, valid_dag: ValidDag, response: Response) -> Tuple[str, Response]:
        # TODO: 
        schedule_name: str = ""
        response: Response = response
        self.dag = valid_dag
        
        return (schedule_name, response)
    
    def trigger_schedule(self, schedule_name: str, response: Response, env: MasonEnvironment) -> Response:
        
        #  TODO :  POST /api/experimental/dags/<schedule_name>/dag_runs
        
        
        # if success
        #

            response.add_error("Dag not found.  Run 'register_dag' first.")

        return response

    def delete_schedule(self, schedule_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

    def trigger_schedule_for_table(self, table_name: str, database_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

