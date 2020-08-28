import importlib
import shutil
from os import path
from sys import path as sys_path
from typing import Optional, List, Union

from mason.engines.scheduler.models.dags.valid_dag import ValidDag
from mason.engines.scheduler.models.dags.dag import Dag
from mason.engines.scheduler.models.schedule import InvalidSchedule
from mason.util.string import to_class_case
from mason.util.uuid import uuid4
from mason.configurations.valid_config import ValidConfig
from mason.util.environment import MasonEnvironment
from mason.util.logger import logger
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.workflows.invalid_workflow import InvalidWorkflow
from mason.workflows.valid_workflow import ValidWorkflow
from mason.workflows.workflow_definition import WorkflowDefinition


class Workflow:

    def __init__(self, namespace: str, command: str, name: str, dag: List[dict], supported_schedulers: List[str], description: Optional[str] = None, source_path: Optional[str] = None):
        self.namespace = namespace
        self.command = command
        self.description = description
        self.dag = Dag(namespace, command, dag)
        self.name = name + "_" + str(uuid4())
        self.supported_schedulers = supported_schedulers
        self.source_path = source_path


    def module(self) -> Union[WorkflowDefinition, InvalidWorkflow]:
        if self.source_path:
            sys_path.append(self.source_path)
            spec = importlib.util.spec_from_file_location("module.name", self.source_path.replace("workflow.yaml", "__init__.py"))
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod) # type: ignore
            classname = to_class_case(f"{self.namespace}_{self.command}")
            try:
                operator_class = getattr(mod, classname)()
                if isinstance(operator_class, WorkflowDefinition):
                    return operator_class
                else:
                    return InvalidWorkflow("Invalid Workflow definition.  See workflows/workflow_definition.py")
            except AttributeError as e:
                return InvalidWorkflow(f"Workflow has no attribute {classname}")
        else:
            return InvalidWorkflow(f"Source path not found for workflow: {self.namespace}:{self.command}")

    def validate(self, env: MasonEnvironment, config: ValidConfig, parameters: WorkflowParameters) -> Union[ValidWorkflow, InvalidWorkflow]:
        if config.scheduler.client_name in self.supported_schedulers:
            validated_dag = self.dag.validate(env, parameters)
            if isinstance(validated_dag, ValidDag):
                schedule = config.scheduler.client.validate_schedule(parameters.schedule)
                if isinstance(schedule, InvalidSchedule):
                    return InvalidWorkflow(f"Invalid Workflow - Bad Schedule: {schedule.reason}")
                else:
                    return ValidWorkflow(self.name, validated_dag, config, schedule)
            else:
                return InvalidWorkflow(f"Invalid DAG definition: {validated_dag.reason}")
        else:
            return InvalidWorkflow(f"Scheduler {config.scheduler.client_name or '(None)'} not supported by workflow")

    def register_to(self, workflow_home: str, force: bool = False):
        if self.source_path:
            dir = path.dirname(self.source_path)
            tree_path = ("/").join([workflow_home.rstrip("/"), self.namespace, self.command + "/"])
            if not path.exists(tree_path):
                logger.info(f"Valid workflow definition.  Registering {dir} to {tree_path}")
                shutil.copytree(dir, tree_path)
            else:
                if force:
                    shutil.rmtree(tree_path)
                    logger.info(f"Valid workflow definition.  Registering {dir} to {tree_path}")
                    shutil.copytree(dir, tree_path)
                else:
                    logger.error("Workflow definition already exists")
        else:
            logger.error("Source path not found for workflow.")

