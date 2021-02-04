from typing import List, Union, Optional

from typistry.protos.invalid_object import InvalidObject
from typistry.validators.base import validate_files

from mason.configurations.config import Config
from mason.resources.filter import Filter
from mason.resources.invalid import InvalidResource
from mason.test.support.validations.config import ConfigProto
from mason.util.logger import logger
from mason.workflows.workflow import Workflow
from mason.util.environment import MasonEnvironment
from mason.operators.operator import Operator

def validate_all(env: MasonEnvironment, file: Optional[str] = None) -> List[Union[Operator, Workflow, Config, InvalidResource]]:
    return validate_resource("all", env, file)

def validate_resource(type: str, env: MasonEnvironment, file: Optional[str] = None) -> List[Union[Operator, Config, Workflow, InvalidResource]]:
    filt = Filter()
    all = []
    if filt.type_any(type):
        if filt.type_config(type):
            all += validate_configs(env, file)
        if filt.type_operator(type):
            all += validate_operators(env, file)
        if filt.type_workflow(type):
            all += validate_workflows(env, file)
    else:
        logger.error(f"Resource not supported: {type}")
        
    all = list(map(lambda r: to_resource(r), all))
    return all

def to_resource(r: Union[Operator, Config, Workflow, InvalidObject]) -> Union[Operator, Config, Workflow, InvalidResource]:
    if isinstance(r, InvalidObject):
        return InvalidResource(r)
    else:
        return r
        
def validate_operators(env: MasonEnvironment, file: Optional[str] = None) -> List[Union[Operator, InvalidObject]]:
    # TODO: Assumes the file you pass is the source folder
    if file:
        path = file + "operators/"
    else:
        path = env.state_store.operator_home

    return validate_files(path, env.validation_path, Operator, include_source=True)

def validate_workflows(env: MasonEnvironment, file: Optional[str] = None) -> List[Union[Workflow, InvalidObject]]:
    if file:
        path = file + "workflows/"
    else:
        path = env.state_store.workflow_home

    return validate_files(path, env.validation_path, Workflow, include_source=True)

def validate_configs(env: MasonEnvironment, file: Optional[str] = None, proto_class = ConfigProto) -> List[Union[Config, InvalidObject]]:
    if file:
        path = file + "configs/"
    else:
        path = env.state_store.config_home

    return validate_files(path, env.validation_path, proto_class=proto_class, include_source=True)

