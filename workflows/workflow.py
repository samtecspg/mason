import shutil
from os import path
from typing import Optional, List, Union, Tuple

from clients.response import Response
from configurations.valid_config import ValidConfig
from operators.operator import Operator
from util.environment import MasonEnvironment
from util.logger import logger
from workflows.dags import validate_dag, Dag


class Workflow:

    def __init__(self, namespace: str, command: str, dag: List[dict], description: Optional[str] = None, parameters: Optional[dict] = None, source_path: Optional[str] = None):
        self.namespace = namespace
        self.command = command
        self.description = description
        self.dag = validate_dag(dag)
        # self.parameters = validate_parameters(parameters)
        self.source_path = source_path

    def validate(self, operators: List[Operator]) -> Union[bool, str]:
        validation: Union[bool, str] = True
        dag = self.dag
        if dag:
            validation = self.validate_operators(dag, operators)
        else:
            validation = "Workflow invalid due to invalid DAG definition"
        return validation

    def validate_operators(self, dag: Dag, operators: List[Operator]) -> Union[bool, str]:
        validation: Union[bool, str] = True
        operator_signatures: List[Tuple[str, str]] = list(map(lambda o: (o.namespace, o.command), operators))
        for step in dag.steps:
            if not (step.namespace,step.command) in operator_signatures:
                validation = f"Unsupported DAG step: Operator {step.namespace} {step.command} not found"
                break
        return validation

    def register_to(self, workflow_home: str):
        if self.source_path:
            dir = path.dirname(self.source_path)
            tree_path = ("/").join([workflow_home.rstrip("/"), self.namespace, self.command + "/"])
            if not path.exists(tree_path):
                shutil.copytree(dir, tree_path)
            else:
                logger.error("Workflow definition already exists")
        else:
            logger.error("Source path not found for operator, run validate_operators to populate")

    def run(self, env: MasonEnvironment, config: ValidConfig, response: Response, dry_run: bool = True) -> Response:
        if dry_run:
            response.add_info("Not Running Workflow")
        else:
            response.add_info("Running Workflow")

        return response

