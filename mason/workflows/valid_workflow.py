from typing import Optional

from mason.clients.engines.scheduler import SchedulerClient

from mason.clients.response import Response
from mason.configurations.valid_config import ValidConfig
from mason.engines.scheduler.models.schedule import Schedule

from mason.util.environment import MasonEnvironment
from mason.engines.scheduler.models.dags import ValidDag

class ValidWorkflow:

    def __init__(self, name: str, dag: ValidDag, config: ValidConfig, schedule: Optional[Schedule]):
        self.name = name
        self.schedule = schedule
        self.dag = dag
        self.config = config

    def run(self, env: MasonEnvironment, response: Response, dry_run: bool = True, run_now: bool = False, schedule_name: Optional[str] = None) -> Response:
        if dry_run:
            response = self.dry_run(env, response)
        else:
            response = self.deploy(env, response, run_now, schedule_name)

        return response

    def dry_run(self, env: MasonEnvironment, response: Response) -> Response:
        response.add_info(f"Performing Dry Run for Workflow.  To Deploy workflow use --deploy -d flag.  To run now use the --run -r flag")
        response.add_info("")
        response.add_info(f"Valid Workflow DAG Definition:")
        response.add_info(f"-" * 80)
        response.add_info(f"{self.dag.display()}")
        response.add_info("")
        return response

    def deploy(self, env: MasonEnvironment, response: Response, run_now: bool, schedule_name: Optional[str] = None) -> Response:
        scheduler = self.config.scheduler
        if isinstance(scheduler.client, SchedulerClient):
            name = schedule_name or self.name
            response.add_info(f"Registering workflow dag {name} with {scheduler.client_name}.")
            schedule_id, response = scheduler.client.register_dag(name, self.dag, response)
            if not response.errored():
                response.add_info(f"Registered schedule {schedule_id}")
            if run_now:
                response.add_info(f"Triggering schedule: {schedule_id}")
                response = scheduler.client.trigger_schedule(schedule_id, response)
        else:
            response.add_error("Scheduler client not defined")

        return response





