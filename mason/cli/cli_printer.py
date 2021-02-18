from typing import List, Union, Optional

from tabulate import tabulate
from typistry.protos.invalid_object import InvalidObject

from mason.clients.response import Response
from mason.configurations.config import Config
from mason.operators.operator import Operator
from mason.resources.malformed import MalformedResource
from mason.workflows.workflow import Workflow
from mason.resources.printer import Printer
from mason.util.list import sequence_4
from mason.util.logger import logger
from mason.util.printer import banner

class CliPrinter(Printer):

    def print_resources(self, resources: List[Union[Operator, Workflow, Config, MalformedResource]], type: Optional[str] = None, namespace: Optional[str] = None, command: Optional[str] = None) -> Response:
        if len(resources) == 0:
            logger.error(self.none_message(type, namespace, command))
        else:
            operators, workflows, configs, bad = sequence_4(resources, Operator, Workflow, Config, MalformedResource)
            type_name = type or "all"
            # TODO: dry up with resources
            if type in ["all", "operator", "operators"]:
                self.print_operators(operators, namespace, command)
            if type in ["all", "workflow", "workflows"]:
                self.print_workflows(workflows, namespace, command)
            if type in ["all", "config", "configs"]:
                self.print_configs(configs)
                
        return Response()

    def print_operators(self, operators: List[Operator], namespace: Optional[str] = None, command: Optional[str] = None):
        operators.sort(key=lambda o: o.namespace)
        
        if len(operators) > 0:
            to_values = list(map(lambda op: [op.namespace, op.command, op.description], operators))
            namesp = f"Operator "
            if namespace:
                namesp += f"{namespace}"

            banner(f"Available {namesp} Methods")
            logger.info()
            logger.info(tabulate(to_values, headers=["namespace", "command", "description"]))
            logger.info()
        else:
            logger.error("No operators registered.  Register operators by running \"mason apply\"")

    def print_workflows(self, workflows: List[Workflow], namespace: Optional[str] = None, command: Optional[str] = None):
        workflows.sort(key=lambda o: o.namespace)
        
        if len(workflows) > 0:
            to_values = list(map(lambda wf: [wf.namespace, wf.command, wf.description], workflows))
            namesp = f"Workflow "
            if namespace:
                namesp += f"{namespace}"

            banner(f"Available {namesp} Methods")
            logger.info()
            logger.info(tabulate(to_values, headers=["namespace", "command", "description"]))
            logger.info()
        else:
            logger.error("No workflows registered.  Register operators by running \"mason apply\"")

    def print_configs(self, configs: List[Config]):
        configs.sort(key=lambda o: o.id)
        
        if len(configs) > 0:
            to_values = list(map(lambda c: c.extended_info(), configs))
            banner(f"Available Configs")
            logger.info()
            logger.info(tabulate(to_values, headers=["id", "execution", "metastore", "storage", "scheduler"]))
            logger.info()
        else:
            logger.error("No configs.  Register configs by running \"mason apply\"")

    def print_invalid(self, invalid: List[InvalidObject]):
        for i in invalid:
            logger.error(i.message)

