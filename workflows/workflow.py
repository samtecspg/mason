import shutil
from os import path
from typing import Optional, List, Union

from util.uuid import uuid4

from configurations.valid_config import ValidConfig
from engines.scheduler.models.schedule import Schedule
from parameters import InputParameters, Parameters, WorkflowParameters
from util.environment import MasonEnvironment
from util.logger import logger
from engines.scheduler.models.dags import Dag, ValidDag
from workflows.invalid_workflow import InvalidWorkflow
from workflows.valid_workflow import ValidWorkflow


class Workflow:

    #  Workflow -> [Workflow, InvalidWorkflow] -> [ValidWorkflow, InvalidWorkflow]

    def __init__(self, namespace: str, command: str, name: str, dag: List[dict], supported_schedulers: List[str], schedule: Optional[str] = None, description: Optional[str] = None, source_path: Optional[str] = None):
        self.namespace = namespace
        self.command = command
        self.description = description
        self.dag = Dag(dag)
        self.name = name + "_" + str(uuid4())
        self.supported_schedulers = supported_schedulers
        self.schedule = self.validate_schedule(schedule)
        self.source_path = source_path

    def validate_schedule(self, schedule: Optional[str]) -> Optional[Schedule]:
        # TODO: Validate that schedule is valid cron definition
        if schedule:
            return Schedule(schedule)
        else:
            return None

    def validate(self, env: MasonEnvironment, config: ValidConfig, parameters: WorkflowParameters) -> Union[ValidWorkflow, InvalidWorkflow]:
        if config.scheduler.client_name in self.supported_schedulers:
            validated_dag = self.dag.validate(env, parameters)
            if isinstance(validated_dag, ValidDag):
                return ValidWorkflow(self.name, validated_dag, config, self.schedule)
            else:
                return InvalidWorkflow(f"Invalid DAG definition: {validated_dag.reason}")
        else:
            return InvalidWorkflow(f"Scheduler {config.scheduler.client_name} not supported by workflow")

    def register_to(self, workflow_home: str):
        if self.source_path:
            dir = path.dirname(self.source_path)
            tree_path = ("/").join([workflow_home.rstrip("/"), self.namespace, self.command + "/"])
            if not path.exists(tree_path):
                shutil.copytree(dir, tree_path)
            else:
                logger.error("Workflow definition already exists")
        else:
            logger.error("Source path not found for workflow.")

