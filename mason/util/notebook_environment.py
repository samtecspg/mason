from typing import Dict, Optional, List

from mason.operators.operator_response import OperatorResponse
from mason.clients.response import Response
from mason.configurations.configurations import get_all, tabulate_configs
from mason.configurations.invalid_config import InvalidConfig
from mason.configurations.valid_config import ValidConfig
from mason.definitions import from_root
from mason.operators.namespaces import to_ops
from mason.operators.operator import Operator
from mason.parameters.input_parameters import InputParameters
from mason.util.environment import MasonEnvironment
from mason.operators.operators import get_operator, list_namespaces, tabulate_operators
from mason.util.logger import logger

class NotebookEnvironment:
    
    def __init__(self):
        env = MasonEnvironment(operator_home=from_root("/examples/operators/"), operator_module="mason.examples.operators", config_home=from_root("/examples/configs/"))
        self.env = env
        valid, invalid = get_all(self.env)
        self.configs: Dict[str, ValidConfig] = valid
        self.invalid_configs: List[InvalidConfig] = invalid
        namespaces, invalid_op = list_namespaces(self.env.operator_home)
        self.namespaces = namespaces
        self.operators = to_ops(namespaces)
        self.invalid_operators = invalid_op
        self.print()
        
    def print(self):
        self.set_display()
        tabulate_configs(self.configs, self.env)
        tabulate_operators(self.env, self.namespaces)
        
    def set_display(self):
        from IPython.core.display import display, HTML
        display(HTML("<style>.container { width:100% !important; }</style>"))

    def config(self, id: str) -> Optional[ValidConfig]:
        return self.configs.get(id)
        
    def operator(self, namespace: str, command: str) -> Optional[Operator]:
        return get_operator(self.env.operator_home, namespace, command)
        
    # TODO: similar to operators run, DRY up
    def run(self, namespace: str, command: str, parameters: str, config_id: str, log_level: str = "trace"):
        logger.set_level(log_level)
        response = Response()
        operator = self.operator(namespace, command)
        config = self.config(config_id)
        input_parameters = InputParameters(parameter_string=parameters)
        if operator:
            if config:
                validated = operator.validate(config, input_parameters)
                operator_response = validated.run(self.env, response)
            else:
                operator_response = OperatorResponse(response.add_error(f"Config {config_id} not found.  Valid config_id's: {', '.join(list(self.configs.keys()))}"))
        else:
            operator_response = response.add_error(f"Operator {namespace} {command} not found")
            
        return operator_response
