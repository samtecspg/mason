import shutil
from os import path
from typing import Optional, List, Union

from configurations.valid_config import ValidConfig
from engines.scheduler.models.schedule import Schedule
from parameters import InputParameters, Parameters
from util.environment import MasonEnvironment
from util.logger import logger
from engines.scheduler.models.dags import Dag, ValidDag
from workflows.invalid_workflow import InvalidWorkflow
from workflows.valid_workflow import ValidWorkflow


class Workflow:

    #  Workflow -> [Workflow, InvalidWorkflow] -> [ValidWorkflow, InvalidWorkflow]

    def __init__(self, namespace: str, command: str, name: str, dag: List[dict], schedule: Optional[str] = None, description: Optional[str] = None, parameters: dict = None, source_path: Optional[str] = None):
        self.namespace = namespace
        self.command = command
        self.description = description
        self.dag = Dag(dag)
        self.parameters = Parameters(parameters)
        self.name = name
        self.schedule = self.validate_schedule(schedule)
        self.source_path = source_path

    def validate_schedule(self, schedule: Optional[str]) -> Optional[Schedule]:
        # TODO: Validate that schedule is valid cron definition
        if schedule:
            return Schedule(schedule)
        else:
            return None

    def validate_definition(self) -> Union['Workflow', InvalidWorkflow]:
        validated = self.dag.validate_definition()
        if isinstance(validated, Dag):
            return self
        else:
            return InvalidWorkflow(validated.reason)

    def validate(self, env: MasonEnvironment, config: ValidConfig, parameters: InputParameters) -> Union[ValidWorkflow, InvalidWorkflow]:
        validated_dag = self.dag.validate(env, config, parameters)
        if isinstance(validated_dag, ValidDag):
            validated_parameters = self.parameters.validate(parameters)
            if validated_parameters.has_invalid:
                return InvalidWorkflow(f"Invalid Parameters: {validated_parameters.messages}")
            else:
                return ValidWorkflow(validated_dag, validated_parameters, config)
        else:
            return InvalidWorkflow(f"Invalid DAG definition: {validated_dag.reason}")

    def register_to(self, workflow_home: str):
        validated = self.validate_config()
        if isinstance(validated, Workflow):
            if self.source_path:
                dir = path.dirname(self.source_path)
                tree_path = ("/").join([workflow_home.rstrip("/"), self.namespace, self.command + "/"])
                if not path.exists(tree_path):
                    shutil.copytree(dir, tree_path)
                else:
                    logger.error("Workflow definition already exists")
            else:
                logger.error("Source path not found for workflow.")
        else:
            logger.error("Invalid Workflow: " + validated.reason)

