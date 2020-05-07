import shutil
from os import path
from typing import Optional, List

from clients.response import Response
from configurations import Config
from operators.operator import Operator
from parameters import Parameters
from util.environment import MasonEnvironment
from util.logger import logger


class Workflow:

    def __init__(self, namespace: str, command: str, dag: List[dict], description: Optional[str] = None, source_path: Optional[str] = None):
        self.namespace = namespace
        self.command = command
        self.description = description
        self.dag = validate_dag(dag)
        self.source_path = source_path

    def validate_dag(self, dag: List[dict]) -> List[DagStep]:


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


    def operators_supported(self, operators: List[Operator]):




    def run(self, env: MasonEnvironment, config: Config, response: Response) -> Response:
        response.add_info("Running Workflow")

        return response
