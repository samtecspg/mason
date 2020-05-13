from clients.engines.scheduler import SchedulerClient

from clients.response import Response
from configurations.valid_config import ValidConfig
from parameters import ValidatedParameters

from util.environment import MasonEnvironment
from engines.scheduler.models.dags import ValidDag

class ValidWorkflow:

    def __init__(self, dag: ValidDag, parameters: ValidatedParameters, config: ValidConfig, name: str):
        self.dag = dag
        self.parameters = parameters
        self.config = config
        self.name = name

    def run(self, env: MasonEnvironment, response: Response, dry_run: bool = True, run_now: bool = False) -> Response:
        if dry_run:
            response = self.dry_run(env, response)
        else:
            response = self.deploy(env, response, run_now)

        return response

    def dry_run(self, env: MasonEnvironment, response: Response) -> Response:
        response.add_info(f"Workflow DAG Definition: {self.dag.to_dict()}")
        return response

    def deploy(self, env: MasonEnvironment, response: Response, run_now: bool) -> Response:
        scheduler_client = self.config.scheduler.client
        if isinstance(scheduler_client, SchedulerClient):
            response.add_info(f"Registering workflow dag with {scheduler_client.client_name}.")
            schedule_id, response = scheduler_client.register_dag(self.name, self.dag)
            response.add_info(f"Registered schedule {schedule_id}")
            if run_now:
                response.add_info(f"Triggering schedule: {schedule_id}")
                response = scheduler_client.trigger_schedule(schedule_id, response)
        else:
            response.add_error("Scheduler client not defined")

        return response





