from typing import Optional

from mason.clients.engines.scheduler import SchedulerClient
from mason.clients.local.scheduler import LocalSchedulerClient

from mason.clients.response import Response
from mason.configurations.config import Config
from mason.engines.scheduler.models.schedule import Schedule
from mason.operators.operator_response import OperatorResponse
from mason.resources.valid import ValidResource

from mason.util.environment import MasonEnvironment
from mason.engines.scheduler.models.dags.valid_dag import ValidDag

class ValidWorkflow(ValidResource):

    def __init__(self, name: str, dag: ValidDag, config: Config, schedule: Optional[Schedule]):
        self.name = name
        self.schedule = schedule
        self.dag = dag
        self.config = config

    def dry_run(self, env: MasonEnvironment, response: Response = Response()) -> OperatorResponse:
        response.add_info(f"Performing Dry Run for Workflow")
        response.add_info("")
        response.add_info(f"Valid Workflow DAG Definition:")
        response.add_info(f"-" * 80)
        response.add_info(f"\n{self.dag.display()}")
        response.add_info("Finished")
        for r in list(map(lambda s: s.reason, self.dag.invalid_steps)):
            response.add_warning(r)
        return OperatorResponse(response)

    def run(self, env: MasonEnvironment, response: Response = Response()) -> OperatorResponse:
        scheduler = self.config.scheduler()
        if isinstance(scheduler, SchedulerClient):
            response.add_info(f"Registering workflow dag {self.name} with {scheduler.client.name()}.")
            schedule_id, response, client_dag = scheduler.register_dag(self.name, self.dag, self.schedule, response)
            if not response.errored():
                response.add_info(f"Registered schedule {schedule_id}")
            # TODO: FIX
            # if client_dag and output_path:
            #     with tempfile.NamedTemporaryFile("w", delete=False) as f:
            #         json = client_dag.to_json()
            #         response.add_info(f"Saving client dag to {output_path}")
            #         f.write(json)
            #         f.close()
            #         response = self.config.storage.client.save_to(f.name, output_path, response)
            if self.schedule:
                response.add_warning(f"Triggering workflow off schedule: {self.schedule.definition}")
                
            response.add_info(f"Triggering schedule: {schedule_id}")
            response = scheduler.trigger_schedule(schedule_id, response, env)
        else:
            response.add_error("Scheduler client not defined")

        return OperatorResponse(response)


