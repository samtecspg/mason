from typing import List, Union, Optional

from tabulate import tabulate
from typistry.protos.invalid_object import InvalidObject

from mason.configurations.config import Config
from mason.operators.operator import Operator
from mason.resources.filter import Filter
from mason.util.list import sequence_4
from mason.util.logger import logger
from mason.util.printer import banner
from mason.workflows.workflow import Workflow

class Printer:

    def print_all(self, resources: List[Union[Operator, Workflow, Config, InvalidObject]], namespace: Optional[str] = None):
        self.print_resource("all", resources, namespace)

    def print_resource(self, type: str, resources: List[Union[Operator, Workflow, Config, InvalidObject]], namespace: Optional[str] = None):
        operators, workflows, configs, invalid = sequence_4(resources, Operator, Workflow, Config, InvalidObject)
        filt = Filter()
        if filt.type_any(type):
            if filt.type_operator(type):
                self.print_operators(operators, namespace)
            if filt.type_workflow(type):
                self.print_workflows(workflows, namespace)
            if filt.type_config(type):
                self.print_configs(configs, namespace)
        else:
            logger.error(f"Resource type not suppored: {type}")

    # TODO: DRY these a bit?
    def print_operators(self, operators: List[Operator], namespace: Optional[str] = None):
        operators.sort(key=lambda o: o.namespace)
        
        if len(operators) > 0:
            to_values = list(
                map(lambda op: [op.namespace, op.command, op.description, op.parameters.to_dict()], operators))
            namesp = namespace or "Operator"
            banner(f"Available {namesp} Methods")
            logger.info()
            logger.info(tabulate(to_values, headers=["namespace", "command", "description", "parameters"]))
            logger.info()
        else:
            if namespace:
                logger.error(
                    f"Operator namespace {namespace} not found.  List operators by running \"mason get operators\"")
            else:
                logger.error("No operators registered.  Register operators by running \"mason apply\"")

    def print_workflows(self, workflows: List[Workflow], namespace: Optional[str] = None):
        workflows.sort(key=lambda o: o.namespace)
        
        if len(workflows) > 0:
            to_values = list(map(lambda wf: [wf.namespace, wf.command, wf.description], workflows))
            namesp = namespace or "Workflow"
            banner(f"Available {namesp} Methods")
            logger.info()
            logger.info(tabulate(to_values, headers=["namespace", "command", "description"]))
            logger.info()
        else:
            if namespace:
                logger.error(
                    f"Workflow namespace {namespace} not found.  List workflows by running \"mason get workflows\"")
            else:
                logger.error("No workflows registered.  Register operators by running \"mason apply\"")

    def print_configs(self, configs: List[Config], config_id: Optional[str] = None):
        configs.sort(key=lambda o: o.id)
        
        if len(configs) > 0:
            to_values = list(map(lambda c: c.extended_info(), configs))
            banner(f"Available Configs")
            logger.info()
            logger.info(tabulate(to_values, headers=["id", "execution", "metastore", "storage", "scheduler"]))
            logger.info()
        else:
            if config_id:
                logger.error(
                    f"Config {config_id} not found")
            else:
                logger.error("No configs.  Register configs by running \"mason apply\"")

    def print_invalid(self, invalid: List[InvalidObject]):
        for i in invalid:
            logger.error(i.message)

