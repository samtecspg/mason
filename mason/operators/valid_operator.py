import importlib
from typing import List, Optional, Union

from mason.clients.response import Response
from mason.configurations.config import Config
from mason.operators.invalid_operator import InvalidOperator
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse
from mason.operators.supported_engines import SupportedEngineSet
from mason.parameters.validated_parameters import ValidatedParameters
from mason.resources.valid import ValidResource
from mason.util.environment import MasonEnvironment
from mason.util.exception import message
from mason.util.string import to_class_case


class ValidOperator(ValidResource):

    def __init__(self, namespace: str, command: str, supported_configurations: List[SupportedEngineSet], description: str,  params: ValidatedParameters, config: Config, source_path: Optional[str] = None):
        self.namespace = namespace
        self.command = command
        self.description = description
        self.parameters = params
        self.config = config
        self.supported_configurations = supported_configurations
        self.source_path = source_path

    def type_name(self):
        return str.capitalize(self.namespace) + str.capitalize(self.command)

    def display_name(self):
        return f"{self.namespace}:{self.command}"

    def module(self, env: MasonEnvironment) -> Union[OperatorDefinition, InvalidOperator]:
        operator_path = env.state_store.operator_home + self.namespace + "/" + self.command + "/"
        classname = to_class_case(f"{self.namespace}_{self.command}")
        try:
            spec = importlib.util.spec_from_file_location(f"mason.operator.{self.namespace}.{self.command}", operator_path + "__init__.py") # type: ignore
            mod = importlib.util.module_from_spec(spec) # type: ignore
            if spec:
                spec.loader.exec_module(mod) #type: ignore
                operator_class = getattr(mod, classname)()
                if isinstance(operator_class, OperatorDefinition):
                    return operator_class
                else:
                    return InvalidOperator("Invalid Operator definition.  See operators/operator_definition.py")
            else:
                return InvalidOperator(f"Could not load importlib from {operator_path + '__init__.py'}")
        except AttributeError as e:
            return InvalidOperator(f"Operator has no attribute {classname}")
        except Exception as e:
            return InvalidOperator(f"Error initializing operator module: {message(e)}")

    def execute(self, env: MasonEnvironment, response: Response, dry_run: bool = True) -> OperatorResponse:
        try:
            module = self.module(env)
            if isinstance(module, OperatorDefinition):
                if dry_run:
                    response.add_info(f"Valid Operator: {self.namespace}:{self.command} with specified parameters.")
                    return OperatorResponse(response)
                else:
                    operator_response: OperatorResponse = module.run(env, self.config, self.parameters, response)
            else:
                response.add_error(f"Module does not contain a valid OperatorDefinition. See /examples for sample operator implementations. \n Message: {module.reason}")
                operator_response = OperatorResponse(response)
        except ModuleNotFoundError as e:
            response.add_error(f"Module Not Found: {e}")
            operator_response = OperatorResponse(response)

        return operator_response

    def run(self, env: MasonEnvironment, response: Response=Response()) -> OperatorResponse:
        return self.execute(env, response, False)

    def dry_run(self, env: MasonEnvironment, response: Response = Response()) -> OperatorResponse:
        return self.execute(env, response)

    def to_dict(self):
        return {
            'namespace': self.namespace,
            'command': self.command,
            'description': self.description,
            'parameters': self.parameters,
            'supported_configurations': list(map(lambda x: x.all, self.supported_configurations))
        }
