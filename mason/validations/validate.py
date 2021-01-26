from typing import TypeVar, List, Union, Optional

from typistry.protos.invalid_object import InvalidObject
from typistry.validators.base import validate_files

from mason.configurations.config import Config
from mason.configurations.invalid_config import InvalidConfig
from mason.test.support.validations.config import ConfigProto
from mason.workflows.workflow import Workflow
from mason.util.environment import MasonEnvironment
from mason.operators.operator import Operator

T = TypeVar("T")
def validate_all(env: MasonEnvironment):
    return validate_operators(env), validate_workflows(env), validate_configs(env)

def validate_operators(env: MasonEnvironment) -> List[Union[Operator, InvalidObject]]:
    return validate_files(env.operator_home, env.validation_path, Operator)

def validate_workflows(env: MasonEnvironment) -> List[Union[Workflow, InvalidObject]]:
    return validate_files(env.workflow_home, env.validation_path, Workflow)

def validate_configs(env: MasonEnvironment, proto_class = ConfigProto) -> List[Union[Config, InvalidObject]]:
    return validate_files(env.config_home, env.validation_path, proto_class=proto_class)
    



