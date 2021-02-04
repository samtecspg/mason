from typing import Union, List, Optional

from mason.configurations.config import Config
from mason.operators.operator import Operator
from mason.resources.invalid import InvalidResource
from mason.workflows.workflow import Workflow

class Filter:
    
    AllResources = Union[Operator, Config, Workflow, InvalidResource]
    
    def type_operator(self, type: str) -> bool:
        return type.lower() in ["all", "operator", "operators"]

    def type_workflow(self, type: str) -> bool:
        return type.lower() in ["all", "workflow", "workflows"]

    def type_config(self, type: str) -> bool:
        return type.lower() in ["all", "config", "configs"]
    
    def type_any(self, type: str) -> bool:
        return self.type_config(type) or self.type_workflow(type) or self.type_operator(type)

    def get_resource(self, type: str, resources: List[AllResources], namespace: str, command: Optional[str] = None) -> Optional[AllResources]:
        filtered = self.filter_resources(type, resources, namespace, command)
        if len(filtered) == 1:
            return filtered[0]
        elif len(filtered) > 1:
            raise Exception(f"Multiple resources match definition: {type}:{namespace}:{command}") 
        else:
            return None
    
    def filter_resources(self, type: str, resources: List[AllResources], namespace: Optional[str], command: Optional[str] = None) -> List[AllResources]:
        return [r for r in resources if self.matches(type, r, namespace, command)]

    def matches(self, type: str, resource: AllResources, namespace: str, command: Optional[str] = None) -> bool:
        if isinstance(resource, Operator) and self.type_operator(type):
            if (namespace == None) or (resource.namespace == namespace):
                return (command == None) or (resource.command == command) 
            else:
                return False
        elif isinstance(resource, Workflow) and self.type_workflow(type):
            if (namespace == None) or (resource.namespace == namespace):
                return (command == None) or (resource.command == command)
            else:
                return False
        elif isinstance(resource, Config) and self.type_config(type):
            if (namespace == None) or (resource.id == namespace):
                return True
        else:
            return False

