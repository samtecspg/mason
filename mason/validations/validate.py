from typing import TypeVar, List, Union, Optional

from typistry.protos.invalid_object import InvalidObject
from typistry.validators.base import validate_files

from mason.configurations.config import Config
from mason.test.support.validations.config import ConfigProto
from mason.workflows.workflow import Workflow
from mason.util.environment import MasonEnvironment
from mason.operators.operator import Operator

T = TypeVar("T")
def validate_all(env: MasonEnvironment, file: Optional[str] = None) -> List[Union[Operator, Workflow, Config, InvalidObject]]:
    operators = validate_operators(env, file)
    workflows = validate_workflows(env, file)
    configs = validate_configs(env, file)
    return operators + workflows + configs

def validate_operators(env: MasonEnvironment, file: Optional[str] = None) -> List[Union[Operator, InvalidObject]]:
    # TODO: Assumes the file you pass is the source folder
    if file:
        path = file + "operators/"
    else:
        path = env.state_store.config_home

    return validate_files(path, env.validation_path, Operator, include_source=True)

def validate_workflows(env: MasonEnvironment, file: Optional[str] = None) -> List[Union[Workflow, InvalidObject]]:
    if file:
        path = file + "workflows/"
    else:
        path = env.state_store.config_home

    return validate_files(path, env.validation_path, Workflow, include_source=True)

def validate_configs(env: MasonEnvironment, file: Optional[str] = None, proto_class = ConfigProto) -> List[Union[Config, InvalidObject]]:
    if file:
        path = file + "configs/"
    else:
        path = env.state_store.config_home

    return validate_files(path, env.validation_path, proto_class=proto_class, include_source=True)




