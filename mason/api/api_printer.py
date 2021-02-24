from typing import List, Union, Optional

from mason.clients.response import Response
from mason.configurations.config import Config
from mason.operators.operator import Operator
from mason.resources.malformed import MalformedResource
from mason.resources.printer import Printer
from mason.util.environment import MasonEnvironment
from mason.util.list import sequence_4
from mason.workflows.workflow import Workflow

class ApiPrinter(Printer):
    
    def print_response(self, response: Response):
        return response.with_status()
    
    def print_resources(self, resources: List[Union[Operator, Workflow, Config, MalformedResource]], type: Optional[str] = None, namespace: Optional[str] = None, command: Optional[str] = None, environment: Optional[MasonEnvironment] = None) -> Response:
        operators, workflows, configs, bad = sequence_4(resources, Operator, Workflow, Config, MalformedResource)
        response = Response()

        if len(resources) == 0:
            response.add_error(self.none_message(type, namespace, command))
            response.set_status(404) 
        else:
            if len(operators) > 0:
                response.add("Operators", list(map(lambda o: o.to_dict(), operators)))
            if len(configs) > 0:
                response.add("Configs", list(map(lambda c: c.to_dict(), configs)))
            if len(workflows) > 0:
                response.add("Workflows", list(map(lambda w: w.to_dict(), workflows)))
            if len(bad) > 0:
                response.add("Errors", list(map(lambda b: b.get_message(), bad)))
                if len(operators + configs + workflows) == 0: # type: ignore
                    response.set_status(400)


        return response
