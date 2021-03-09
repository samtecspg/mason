from functools import lru_cache
from typing import Optional, List, Union, TypeVar, Any, Type, Tuple

from typistry.protos.invalid_object import InvalidObject
from typistry.validators.base import validate_files

from mason.configurations.config import Config
from mason.operators.operator import Operator
from mason.parameters.operator_parameters import OperatorParameters
from mason.parameters.parameters import Parameters
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.util.list import sequence_4
from mason.resources.malformed import MalformedResource
from mason.util.environment import MasonEnvironment
from mason.workflows.workflow import Workflow

class Resources:

    def __init__(self, env: MasonEnvironment):
        self.env = env

    def type_any(self, type: str) -> bool:
        return self.type_operator(type) or self.type_workflow(type) or self.type_config(type) or self.type_bad(type)
    def type_operator(self, type: str) -> bool:
        return type.lower() in ["all", "operator", "operators"]
    def type_workflow(self, type: str) -> bool:
        return type.lower() in ["all", "workflow", "workflows"]
    def type_config(self, type: str) -> bool:
        return type.lower() in ["all", "config", "configs"]
    def type_bad(self, type: str) -> bool:
        return type.lower() in ["bad", "invalid", "malformed"]

    @lru_cache
    def get_all(self, file: Optional[str] = None) -> List[Union[Operator, Workflow, Config, MalformedResource]]:
        path = file or self.env.mason_home
        validated = validate_files(path, self.env.validation_path, include_source=True)
        return self.to_resources(validated)
    
    def get_resources(self, type: str, namespace: Optional[str] = None, command: Optional[str] = None) -> List[Union[Operator, Workflow, Config, MalformedResource]]:
        all: List[Union[Operator, Workflow, Config, MalformedResource]] = []
        if self.type_any(type):
            if self.type_operator(type):
                if namespace and command:
                    all += [self.get_operator(namespace, command)]
                else:
                    all += self.get_operators(namespace, command)
                    all += self.get_bad()
            if self.type_workflow(type):
                if namespace and command:
                    all += [self.get_workflow(namespace, command)]
                else:
                    all += self.get_workflows(namespace, command)
                    all += self.get_bad()
            if self.type_config(type):
                if namespace:
                    all += [self.get_config(namespace)]
                else:
                    all += self.get_configs(namespace)
                    all += self.get_bad()
        else:
            all += [MalformedResource(message=f"Unsupported type: {type}")]
        relevant = [a for a in all if not(isinstance(a, MalformedResource) and a.ignorable())]
        return relevant
        
    def get_operators(self, namespace: Optional[str] = None, command: Optional[str] = None) -> List[Operator]:
        ops, bad = self.filter_resource(Operator, namespace, command)
        return ops
    
    def get_operator(self, namespace: str, command: str) -> Union[Operator, MalformedResource]:
        op = self.get_resource("operator", namespace, command)
        if isinstance(op, Operator) or isinstance(op, MalformedResource):
            return op
        else:
            return MalformedResource(message="Bad type cast")

    def get_workflows(self, namespace: Optional[str] = None, command: Optional[str] = None) -> List[Workflow]:
        workflows, bad = self.filter_resource(Workflow, namespace, command)
        return workflows

    def get_workflow(self, namespace: str, command: str) -> Union[Workflow, MalformedResource]:
        op = self.get_resource("workflow", namespace, command)
        if isinstance(op, Workflow) or isinstance(op, MalformedResource):
            return op
        else:
            return MalformedResource(message="Bad type cast")

    def get_configs(self, config_id: Optional[str] = None) -> List[Config]:
        configs, bad = self.filter_resource(Config, config_id)
        return configs

    def get_config(self, config_id: str) -> Union[Config, MalformedResource]:
        conf: Union[Operator, Config, Workflow, MalformedResource] = self.get_resource("config", config_id)
        if isinstance(conf, Config) or isinstance(conf, MalformedResource):
            return conf
        else:
            return MalformedResource(message="Bad type cast")
        
    def get_best_config(self, config_id: Optional[str] = None) -> Union[Config, MalformedResource]:
        by_id: Union[Config, MalformedResource, None] = None
        if config_id:
            by_id = self.get_config(config_id)
            if isinstance(by_id, MalformedResource) and by_id.ignorable():
                by_id = None
        best_config: Union[Config, MalformedResource] = by_id or self.get_session_config() or self.get_first_config()
        return best_config
    
    def get_session_config(self) -> Union[Config, MalformedResource, None]:
        config_id = self.env.state_store.get_session_config()
        if config_id:
            session_config = self.get_config(config_id)
            if isinstance(session_config, MalformedResource) and session_config.ignorable():
                return None
            else:
                return session_config
        else:
            return None

    def get_first_config(self) -> Union[Config, MalformedResource]:
        configs: List[Config] = self.get_configs()
        configs.sort(key=lambda c: c.id)
        if len(configs) > 0:
            return configs[0]
        else:
            return MalformedResource()

    def set_session_config(self, config_id: str) -> Union[Config, str]:
        config = self.get_config(config_id)
        if config:
            return self.env.state_store.set_session_config(config_id)
        else:
            return f"Config does not exist: {config_id}"

    def matches(self, resource: Union[Operator, Workflow, Config], namespace: Optional[str] = None, command: Optional[str] = None) -> bool:
        matches = False
        if isinstance(resource, Operator):
            if (namespace == None) or (resource.namespace == namespace):
                if (command == None) or (resource.command == command):
                    matches = True
        elif isinstance(resource, Workflow):
            if (namespace == None) or (resource.namespace == namespace):
                if (command == None) or (resource.command == command):
                    matches = True
        else:
            if (namespace == None) or (resource.id == namespace):
                matches = True
        return matches
                
    def to_resources(self, resources: List[Union[InvalidObject, Any]]) -> List[Union[Operator, Workflow, Config, MalformedResource]]:
        return [self.to_resource(r) for r in resources]

    def to_resource(self, resource: Union[InvalidObject, Any]) -> Union[Operator, Workflow, Config, MalformedResource]:
        if isinstance(resource, InvalidObject):
            return MalformedResource(resource)
        elif isinstance(resource, Operator) or isinstance(resource, Workflow) or isinstance(resource, Config):
            return resource
        else:
            return MalformedResource(message=f"Not a valid resource type: {resource.__class__.__name__}")
    
    def get_resource(self, type: str, namespace: Optional[str] = None, command: Optional[str] = None) -> Union[Operator, Workflow, Config, MalformedResource]:
        resources: List[Union[Operator, Workflow, Config]] = []
        bad: List[MalformedResource] = []
        if self.type_operator(type):
            resources, bad = self.filter_resource(Operator, namespace, command)
        elif self.type_workflow(type):
            resources, bad = self.filter_resource(Workflow, namespace, command)
        elif self.type_config(type):
            resources, bad = self.filter_resource(Config, namespace, command)
        else:
            bad = [MalformedResource(message=f"Unsupported resource type: {type}")]
            
        if len(resources) > 1:
            message = f"Multiple {type} resources matching {namespace}"
            if command:
                message += f":{command}"
            return MalformedResource(message=message)
        elif len(resources) == 0:
            matching_bad = self.get_matching_bad_resource(type, bad, namespace, command)
            if isinstance(matching_bad, MalformedResource):
                return matching_bad
            else:
                not_found = f"{type}"
                if namespace:
                    not_found += f":{namespace}"
                if command:
                    not_found += f":{command}"
                return MalformedResource(not_found=not_found)
        else:
            return resources[0]
        
    def get_bad(self) -> List[MalformedResource]:
        blank, bad = self.filter_resource(MalformedResource)
        return bad

    def get_matching_bad_resource(self, type: str, bad: List[MalformedResource], namespace: Optional[str] = None, command: Optional[str] = None) -> Optional[MalformedResource]:
        relevant_bad = None
        if self.type_operator(type) or self.type_workflow(type):
            if namespace and command:
                for b in bad:
                    try:
                        obj = b.invalid_obj
                        if obj:
                            if obj.reference.attributes().get("namespace") == namespace and obj.reference.attributes().get("command") == command:
                                relevant_bad = b
                    except Exception as e:
                        pass

        elif self.type_config(type):
            if namespace:
                for b in bad:
                    try:
                        obj = b.invalid_obj
                        if obj:
                            if obj.reference.attributes().get("id") == namespace:
                                relevant_bad = b
                    except Exception as e:
                        pass 
                
        return relevant_bad

    T = TypeVar("T", bound="Resource")
    def filter_resource(self, cls: Type[T], namespace: Optional[str] = None, command: Optional[str] = None) -> Tuple[List[T], List[MalformedResource]]:
        all = self.get_all()
        operators, workflows, configs, malformed = sequence_4(all, Operator, Workflow, Config, MalformedResource)
        if cls == Operator:
            ops = [o for o in operators if self.matches(o, namespace, command)]
            return ops, malformed # type: ignore
        elif cls == Workflow:
            wfs = [w for w in workflows if self.matches(w, namespace, command)]
            return wfs, malformed # type: ignore
        elif cls == Config:
            cfg = [c for c in configs if self.matches(c, namespace, command)]
            return cfg, malformed # type: ignore
        elif cls == MalformedResource:
            return [], malformed
        else:
            return [], [MalformedResource(message=f"Undefined resource: {cls}")]

    def get_parameters(self, type: str, parameter_string: Optional[str], parameter_path: Optional[str], parameter_dict: Optional[dict]) -> Union[Parameters, MalformedResource]:
        parameters: Union[Parameters, MalformedResource]
        if self.type_workflow(type):
            parameters = WorkflowParameters(parameter_path, parameter_dict)
        elif self.type_operator(type):
            parameters = OperatorParameters(parameter_string, parameter_path, parameter_dict)
        elif self.type_config(type):
            parameters = MalformedResource(message=f"Config type not supported: {type}")
        else:
            parameters = MalformedResource(message=f"Type not supported: {type}")

        return parameters