from typing import Optional, List, Union

from typistry.protos.invalid_object import InvalidObject
from typistry.validators.base import validate_files

from mason.configurations.config import Config
from mason.operators.operator import Operator
from mason.parameters.parameters import Parameters
from mason.parameters.operator_parameters import OperatorParameters
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.resources.malformed import MalformedResource
from mason.util.environment import MasonEnvironment
from mason.util.list import sequence
from mason.util.logger import logger
from mason.workflows.workflow import Workflow

def type_operator(type: str) -> bool:
    return type.lower() in ["all", "operator", "operators"]

def type_workflow(type: str) -> bool:
    return type.lower() in ["all", "workflow", "workflows"]

def type_config(type: str) -> bool:
    return type.lower() in ["all", "config", "configs"]

def type_any(type: str) -> bool:
    return type_config(type) or type_workflow(type) or type_operator(type)

def get_all(env: MasonEnvironment, file: Optional[str] = None) -> List[Union[Operator, Workflow, Config, MalformedResource]]:  
    return get_resources("all", env, file)

def get_resource(type: str, env: MasonEnvironment, namespace: str, command: Optional[str] = None, all: Optional[List[Union[Operator, Workflow, Config, MalformedResource]]] = None) -> Union[Operator, Workflow, Config, MalformedResource]:
    resources: List[Union[Operator, Workflow, Config, MalformedResource]] = all or get_resources(type, env, None, namespace, command)
    if len(resources) == 0:
        message = f"No {type} resource matching {namespace}"
        if command:
            message += f":{command}"
        return MalformedResource(message=message)
    elif len(resources) > 1:
        message = f"Multiple {type} resources matching {namespace}"
        if command:
            message += f":{command}"
        return MalformedResource(message=message)
    else:
        return resources[0]

def get_config(env: MasonEnvironment, config_id: str, all) -> Union[Config, MalformedResource]:
    config = get_resource("config", env, config_id, all=all)
    if isinstance(config, Config) or isinstance(config, MalformedResource):
        return config
    else:
        return MalformedResource(message="Bad type cast")
    
def get_operator(env: MasonEnvironment, namespace: str, command: str):
    operator = get_resource("operator", env, namespace, command)
    if isinstance(operator, Operator) or isinstance(operator, MalformedResource):
        return operator
    else:
        return MalformedResource(message="Bad type cast")

def get_workflow(env: MasonEnvironment, namespace: str, command: str):
    workflow = get_resource("workflow", env, namespace, command)
    if isinstance(workflow, Workflow) or isinstance(workflow, MalformedResource):
        return workflow
    else:
        return MalformedResource(message="Bad type cast")

def get_best_config(env: MasonEnvironment, config_id: Optional[str]) -> Union[Config, MalformedResource]:
    all_configs: List[Union[Config, MalformedResource]] = get_resources("config", env, namespace=config_id)
    if config_id:
        config = get_config(env, config_id, all_configs)
    else:
        config = MalformedResource(message="No config id")
        
    if isinstance(config, MalformedResource):
        config = get_session_config(env, all_configs)
        if isinstance(config, MalformedResource):
            config = get_first_config(all_configs)
            
    return config
             
def get_resources(type: str, env: MasonEnvironment, file: Optional[str] = None, namespace: Optional[str] = None, command: Optional[str] = None) -> List[Union[Operator, Workflow, Config, MalformedResource]]:
    all: List[Union[Operator, Workflow, Config, MalformedResource]] = []
    if type_operator(type):
        all += get_operators(env, file, namespace, command)
    if type_workflow(type):
        all += get_workflows(env, file, namespace, command)
    if type_config(type):
        all += get_configs(env, file, namespace)
        
    relevant = [a for a in all if not (isinstance(a, MalformedResource) and a.ignorable())]
        
    return relevant

def get_operators(env: MasonEnvironment, file: Optional[str] = None, namespace: Optional[str] = None, command: Optional[str] = None) -> List[Union[Operator, MalformedResource]]:
    if file:
        path = file + "operators/"
    else:
        path = env.state_store.operator_home

    operators: List[Union[Operator, InvalidObject]] = validate_files(path, env.validation_path, include_source=True)
    return list(map(lambda r: filter_operator(r, namespace, command), operators))

def get_workflows(env: MasonEnvironment, file: Optional[str] = None, namespace: Optional[str] = None, command: Optional[str] = None) -> List[Union[Workflow, MalformedResource]]:
    if file:
        path = file + "workflows/"
    else:
        path = env.state_store.workflow_home

    workflows: List[Union[Workflow, InvalidObject]] = validate_files(path, env.validation_path, include_source=True)
    return list(map(lambda r: filter_workflow(r, namespace, command), workflows))

def get_configs(env: MasonEnvironment, file: Optional[str] = None, config_id: Optional[str] = None) -> List[Union[Config, MalformedResource]]:
    if file:
        path = file + "configs/"
    else:
        path = env.state_store.config_home

    configs: List[Union[Config, InvalidObject]] = validate_files(path, env.validation_path, include_source=True)
    return list(map(lambda r: filter_config(r, config_id), configs))

    
def filter_operator(operator: Union[Operator, InvalidObject], namespace: Optional[str] = None, command: Optional[str] = None) -> Union[Operator, MalformedResource]:
    if isinstance(operator, InvalidObject):
        return MalformedResource(operator)
    else:
        if (namespace == None) or (operator.namespace == namespace):
            if (command == None) or (operator.command == command):
                return operator
            else:
                return MalformedResource()
        else:
            return MalformedResource()

def filter_workflow(workflow: Union[Workflow, InvalidObject], namespace: Optional[str] = None, command: Optional[str] = None) -> Union[Workflow, MalformedResource]:
    if isinstance(workflow, InvalidObject):
        return MalformedResource(workflow)
    else:
        if (namespace == None) or (workflow.namespace == namespace):
            if (command ==None) and (workflow.command == command):
                return workflow
            else:
                return MalformedResource()
        else:
            return MalformedResource()


def filter_config(config: Union[Config, InvalidObject], namespace: Optional[str] = None, command: Optional[str] = None) -> Union[Config, MalformedResource]:
    if isinstance(config, InvalidObject):
        return MalformedResource(config)
    else:
        if (namespace == None) or (config.id == namespace):
            return config
        else:
            return MalformedResource()

def set_session_config(env: MasonEnvironment, config_id: str) -> Optional[Config]:
    all_configs = get_configs(env)
    config = get_config(env, config_id, all_configs)
    if config:
        return env.state_store.set_session_config(config_id)
    else:
        logger.error(f"Config does not exist: {config_id}")
        return None

def get_session_config(env: MasonEnvironment, configs: List[Union[Config, InvalidObject]]) -> Union[Config, MalformedResource]:
    config_id = env.state_store.get_session_config()
    if config_id:
        return get_config(env, config_id, configs)
    else:
        return MalformedResource(message="No session config")
        
def get_first_config(configs: List[Union[Config, MalformedResource]]) -> Union[Config, MalformedResource]:
    valid, invalid = sequence(configs, Config, MalformedResource)
    valid.sort(key=lambda c: c.id)
    if len(valid) > 0:
        return valid[0]
    else:
        return MalformedResource(message="No Valid Configs")

def get_parameters(type: str, parameter_string: Optional[str], parameter_path: Optional[str]) -> Union[Parameters, MalformedResource]:
    parameters: Union[Parameters, MalformedResource] = MalformedResource(message=f"Type not supported: {type}")
    if type_any(type):
        if type_workflow(type):
            parameters = WorkflowParameters(parameter_path)
        elif type_operator(type):
            parameters = OperatorParameters(parameter_string, parameter_path)
        elif type_config(type):
            parameters = MalformedResource(message=f"Config type not supported: {type}")
        
    return parameters
        