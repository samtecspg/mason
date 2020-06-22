from botocore.client import BaseClient
from typing import Tuple

from mason.engines.scheduler.models.dags.valid_dag import ValidDag
from mason.util.environment import MasonEnvironment
from mason.clients.response import Response
from mason.workflows.workflow_run import WorkflowRun


class LocalClient:

    def __init__(self, config: dict):
        self.threads = config.get("threads")

    def client(self) -> BaseClient:
        pass

    def register_dag(self, schedule_name: str, valid_dag: ValidDag, response: Response) -> Tuple[str, Response]:
        response.add_info("Registering DAG in local memory")
        self.dag = valid_dag
        return (schedule_name, response)

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

