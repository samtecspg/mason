from typing import Optional, List, Union, TypeVar, Any, Type

from typistry.protos.invalid_object import InvalidObject
from typistry.validators.base import validate_files

from mason.configurations.config import Config
from mason.operators.operator import Operator
from mason.parameters.parameters import Parameters
from mason.workflows.workflow import Workflow
from mason.parameters.operator_parameters import OperatorParameters
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.resources.malformed import MalformedResource
from mason.util.environment import MasonEnvironment
from mason.util.list import sequence, sequence_4
from mason.util.logger import logger
from mason.resources.resource import Resource

class Resources:
    def __init__(self, env: MasonEnvironment):
        self.env = env
        self.operators: List[Operator] = []
        self.workflows: List[Config] = []
        self.configs: List[Workflow] = []
        self.bad: List[MalformedResource] = []

    def type_operator(self, type: str) -> bool:
        return type.lower() in ["all", "operator", "operators"]
    def type_workflow(self, type: str) -> bool:
        return type.lower() in ["all", "workflow", "workflows"]
    def type_config(self, type: str) -> bool:
        return type.lower() in ["all", "config", "configs"]
    def type_bad(self, type: str) -> bool:
        return type.lower() in ["bad", "invalid", "malformed"]

    T = TypeVar("T", bound="Resource")
    def get_resources(self, cls: Type[T], file: Optional[str] = None) -> List[Union[T, MalformedResource]]:
        path = file or self.env.state_store.operator_home
        all = validate_files(path, self.env.validation_path, include_source=True)
        resources = [self.to_resource(cls, a) for a in all if not (isinstance(a, MalformedResource) and a.ignorable())]
        if cls == Operator:
            self.operators = resources
        elif cls == Workflow:
            self.workflows = resources

        return resources
    
    def get_resource(self, cls: Type[T], namespace: Optional[str] = None, command: Optional[str] = None) -> Union[T, MalformedResource]:
        pass
        
    def to_resource(self, cls: Type[T], obj: Union[Any, InvalidObject]):
        if isinstance(obj, cls):
            return obj
        elif isinstance(obj, InvalidObject):
            return MalformedResource(obj)
        else:
            return MalformedResource()

    # def get_all(self, file: Optional[str] = None):
    #     self.get_resources("all", file)
    # 
    # def get_resources(self, type: str, file: Optional[str] = None): 
    #     pass

def type_operator(type: str) -> bool:
    return type.lower() in ["all", "operator", "operators"]

def type_workflow(type: str) -> bool:
    return type.lower() in ["all", "workflow", "workflows"]

def type_config(type: str) -> bool:
    return type.lower() in ["all", "config", "configs"]

def type_bad(type: str) -> bool:
    return type.lower() in ["bad", "invalid", "malformed"]

def type_any(type: str) -> bool:
    return type_config(type) or type_workflow(type) or type_operator(type)

def get_all(env: MasonEnvironment, file: Optional[str] = None) -> List[Union[Operator, Workflow, Config, MalformedResource]]:  
    return get_resources_type("all", env, file)

def get_resources_type(type: str, env: MasonEnvironment, file: Optional[str] = None, namespace: Optional[str] = None, command: Optional[str] = None) -> List[Union[Operator, Workflow, Config, MalformedResource]]:
    all: List[Union[Operator, Workflow, Config, MalformedResource]] = []
    if type_operator(type):
        all += get_operators(env, file, namespace, command)
    if type_workflow(type):
        all += get_workflows(env, file, namespace, command)
    if type_config(type):
        all += get_configs(env, file, namespace)

    relevant = [a for a in all if not (isinstance(a, MalformedResource) and a.ignorable())]
    return relevant

def get_resources(type: str, env: MasonEnvironment, namespace: Optional[str] = None, command: Optional[str] = None) -> List[Union[Operator, Workflow, Config, MalformedResource]]:
    resources = []
    if (type_operator(type) or type_workflow(type)) and namespace and command:
        resource = get_resource(type, env, namespace, command)
        if resource:
            resources.append(resource)
    elif type_config(type) and namespace:
        resource = get_resource(type, env, namespace)
        if resource:
            resources.append(resource)
    else:
        resources = get_resources_type(type, env, None, namespace, command)
    return resources
        

def get_resource(type: str, env: MasonEnvironment, namespace: str, command: Optional[str] = None, all: Optional[List[Union[Operator, Workflow, Config, MalformedResource]]] = None) -> Union[Operator, Workflow, Config, MalformedResource, None]:
    resources: List[Union[Operator, Workflow, Config, MalformedResource]] = all or get_resources_type(type, env, None, namespace, command)
    operators, workflows, configs, bad = sequence_4(resources, Operator, Workflow, Config, MalformedResource)
    relevant: List[Union[Operator, Workflow, Config, MalformedResource]] = []
    relevant_malformed: List[MalformedResource] = []

    if type_operator(type) or type_workflow(type):
        for b in bad:
            try:
                obj = b.invalid_obj
                if obj:
                    if obj.reference.attributes().get("namespace") == namespace and obj.reference.attributes().get("command") == command:
                        relevant_malformed.append(b)
            except Exception as e:
                pass
            
    elif type_config(type):
        for b in bad:
            try:
                obj = b.invalid_obj
                if obj:
                    if obj.reference.attributes().get("id") == namespace:
                        relevant_malformed.append(b)
            except Exception as e:
                pass

    if type_operator(type):
        relevant += operators
    elif type_workflow(type):
        relevant += workflows
    elif type_config(type):
        relevant += configs
    elif type_any(type):
        relevant = resources
    
    if len(relevant + relevant_malformed) > 1:
        message = f"Multiple {type} resources matching {namespace}"
        if command:
            message += f":{command}"
        return MalformedResource(message=message)
    elif len(relevant) == 1:
        return relevant[0]
    elif len(relevant_malformed) == 1:
        rm = relevant_malformed[0]
        return MalformedResource(rm.invalid_obj, f"Resource malformed: {rm.get_message()}")
    else:
        return None

def get_operators(env: MasonEnvironment, file: Optional[str] = None, namespace: Optional[str] = None, command: Optional[str] = None) -> List[Union[Operator, MalformedResource]]:
    path = file or env.state_store.operator_home
    operators: List[Union[Operator, InvalidObject]] = validate_files(path, env.validation_path, include_source=True, filter_type="operator")
    return list(map(lambda r: filter_operator(r, namespace, command), operators))

def get_operator(env: MasonEnvironment, namespace: str, command: str) -> Union[Operator, MalformedResource, None]:
    operator = get_resource("operator", env, namespace, command)
    if (operator == None) or isinstance(operator, Operator) or isinstance(operator, MalformedResource):
        return operator
    else:
        return MalformedResource(message="Bad type cast")

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

def get_workflows(env: MasonEnvironment, file: Optional[str] = None, namespace: Optional[str] = None, command: Optional[str] = None) -> List[Union[Workflow, MalformedResource]]:
    path = file or env.state_store.workflow_home
    workflows: List[Union[Workflow, InvalidObject]] = validate_files(path, env.validation_path, include_source=True, filter_type="workflow")
    return list(map(lambda r: filter_workflow(r, namespace, command), workflows))

def get_workflow(env: MasonEnvironment, namespace: str, command: str) -> Union[Workflow, MalformedResource, None]:
    workflow = get_resource("workflow", env, namespace, command)
    if (workflow == None) or isinstance(workflow, Workflow) or isinstance(workflow, MalformedResource):
        return workflow 
    else:
        return MalformedResource(message="Bad type cast")

def filter_workflow(workflow: Union[Workflow, InvalidObject], namespace: Optional[str] = None, command: Optional[str] = None) -> Union[Workflow, MalformedResource]:
    if isinstance(workflow, InvalidObject):
        return MalformedResource(workflow)
    else:
        if (namespace == None) or (workflow.namespace == namespace):
            if (command == None) or (workflow.command == command):
                return workflow
            else:
                return MalformedResource()
        else:
            return MalformedResource()

def get_configs(env: MasonEnvironment, file: Optional[str] = None, config_id: Optional[str] = None) -> List[Union[Config, MalformedResource]]:
    path = file or env.state_store.config_home

    configs: List[Union[Config, InvalidObject]] = validate_files(path, env.validation_path, include_source=True, filter_type="config")
    return list(map(lambda r: filter_config(r, config_id), configs))

def get_config(env: MasonEnvironment, config_id: str, all: Optional[list] = None) -> Union[Config, MalformedResource, None]:
    config = get_resource("config", env, config_id, all=all)
    if (config == None) or isinstance(config, Config) or isinstance(config, MalformedResource):
        return config 
    else:
        return MalformedResource(message="Bad type cast")

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

def get_session_config(env: MasonEnvironment) -> Union[Config, MalformedResource, None]:
    config_id = env.state_store.get_session_config()
    if config_id:
        return get_config(env, config_id)
    else:
        return None

def get_first_config(configs: List[Union[Config, MalformedResource, None]]) -> Union[Config, MalformedResource]:
    valid, invalid = sequence(configs, Config, MalformedResource)
    valid.sort(key=lambda c: c.id)
    if len(valid) > 0:
        return valid[0]
    else:
        return MalformedResource(message="No Valid Configs")

def get_best_config(env: MasonEnvironment, config_id: Optional[str] = None) -> Union[Config, MalformedResource, None]:
    all_configs: List[Union[Config, MalformedResource, None]] = get_resources_type("config", env, namespace=config_id) # type: ignore
    config: Union[Config, MalformedResource, None] = None
    if config_id:
        config = get_config(env, config_id, all_configs)
    return config or get_session_config(env) or get_first_config(all_configs)

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
        