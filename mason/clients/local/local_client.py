from botocore.client import BaseClient
from typing import Tuple, Optional

from mason.clients.base import Client
from mason.engines.scheduler.models.dags.client_dag import ClientDag
from mason.engines.scheduler.models.dags.valid_dag import ValidDag
from mason.engines.scheduler.models.schedule import Schedule
from mason.util.environment import MasonEnvironment
from mason.clients.response import Response
from mason.workflows.workflow_run import WorkflowRun

class LocalClient(Client):

    def __init__(self, threads: int = 1):
        self.threads = threads 

    def client(self) -> BaseClient:
        pass

    def register_dag(self, schedule_name: str, valid_dag: ValidDag, schedule: Optional[Schedule], response: Response) -> Tuple[str, Response, Optional[ClientDag]]:
        response.add_info("Registering DAG in local memory")
        self.dag = valid_dag
        return (schedule_name, response, None)

    def trigger_schedule(self, schedule_name: str, response: Response, env: MasonEnvironment) -> Response:
        dag = self.dag
        if dag:
            workflow_run = WorkflowRun(dag)
            response = workflow_run.run(env, response)
        else:
            response.add_error("Dag not found.  Run 'register_dag' first.")
            
        return response

    def delete_schedule(self, schedule_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

    def trigger_schedule_for_table(self, table_name: str, database_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

