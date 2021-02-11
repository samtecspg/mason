from abc import abstractmethod
from typing import List, Union, Optional

from mason.clients.response import Response
from mason.configurations.config import Config
from mason.operators.operator import Operator
from mason.resources.malformed import MalformedResource
from mason.workflows.workflow import Workflow

class Printer:
    
    def none_message(self, type: Optional[str] = None, namespace: Optional[str] = None, command: Optional[str] = None) -> str:
        if type and type != "all":
            message = f"No {type}"
        else:
            message = "No resources"
        if namespace:
            message += f" matching {namespace}"
        if command:
            message += f":{command}"
        message += ". Register new resources with 'mason apply'"
        return message
    
    @abstractmethod
    def print_resources(self, operators: List[Union[Operator, Workflow, Config, MalformedResource]], type: Optional[str] = None, namespace: Optional[str] = None, command: Optional[str] = None) -> Response:
        raise Exception("print_resources undefined for Printer")