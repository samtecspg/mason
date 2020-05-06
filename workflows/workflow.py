import shutil
from os import path
from typing import Optional

from clients.response import Response
from configurations import Config
from parameters import Parameters
from util.environment import MasonEnvironment
from util.logger import logger

class Workflow:

    def __init__(self, namespace: str, command: str, source_path: Optional[str] = None):
        self.namespace = namespace
        self.command = command

    def register_to(self, workflow_home: str):
        if self.source_path:
            dir = path.dirname(self.source_path)
            tree_path = ("/").join([workflow_home.rstrip("/"), self.cmd, self.subcommand + "/"])
            if not path.exists(tree_path):
                shutil.copytree(dir, tree_path)
            else:
                logger.error("Operator definition already exists")
        else:
            logger.error("Source path not found for operator, run validate_operators to populate")


    def run(self, env: MasonEnvironment, config: Config, response: Response) -> Response:

        # self.validate(config, parameters, response)

        # if not response.errored():
        #     try:
        #         mod = import_module(f'{env.operator_module}.{self.cmd}.{self.subcommand}')
        #         response = mod.run(env, config, parameters, response)  # type: ignore
        #     except ModuleNotFoundError as e:
        #         response.add_error(f"Module Not Found: {e}")
        response.add_info("Running Workflow")

        return response
