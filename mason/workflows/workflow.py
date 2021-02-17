import importlib
from sys import path as sys_path
from typing import Optional, List, Union

from mason.clients.response import Response
from mason.configurations.config import Config
from mason.engines.scheduler.models.dags.dag import Dag
from mason.engines.scheduler.models.dags.valid_dag import ValidDag
from mason.engines.scheduler.models.schedule import InvalidSchedule
from mason.resources.resource import Resource
from mason.resources.saveable import Saveable
from mason.state.base import MasonStateStore
from mason.util.environment import MasonEnvironment
from mason.util.exception import message
from mason.util.string import to_class_case
from mason.util.uuid import uuid4
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.workflows.invalid_workflow import InvalidWorkflow
from mason.workflows.valid_workflow import ValidWorkflow
from mason.workflows.workflow_definition import WorkflowDefinition

class Workflow(Saveable, Resource):

    def __init__(self, namespace: str, command: str, name: str, dag: List[dict], supported_schedulers: List[str], description: Optional[str] = None, source: Optional[str] = None):
        super().__init__(source)
        self.namespace = namespace
        self.command = command
        self.description = description
        self.dag = Dag(namespace, command, dag)
        self.name = name + "_" + str(uuid4())
        self.supported_schedulers = supported_schedulers

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

    def validate(self, env: MasonEnvironment, config: Config, parameters: WorkflowParameters, strict: bool = True) -> Union[ValidWorkflow, InvalidWorkflow]:
        scheduler_client = config.scheduler().client.name()
        if scheduler_client in self.supported_schedulers:
            validated_dag = self.dag.validate(env, parameters, strict)
            if isinstance(validated_dag, ValidDag):
                schedule = config.scheduler().validate_schedule(parameters.schedule)
                if isinstance(schedule, InvalidSchedule):
                    return InvalidWorkflow(f"Invalid Workflow - Bad Schedule: {schedule.reason}")
                else:
                    return ValidWorkflow(parameters.schedule_name or self.name, validated_dag, config, schedule)
            else:
                return InvalidWorkflow(f"Invalid DAG definition: {validated_dag.reason}")
        else:
            return InvalidWorkflow(f"Scheduler {scheduler_client or '(None)'} not supported by workflow")


    def save(self, state_store: MasonStateStore, overwrite: bool = False, response: Response = Response()):
        try:
            state_store.cp_source(self.source_path, "workflow", self.namespace, self.command, overwrite)
            response.add_info(f"Successfully saved workflow {self.namespace}:{self.command}")
        except Exception as e:
            response.add_error(f"Error copying source: {message(e)}")
        return response

    def to_dict(self) -> dict:
        return {}