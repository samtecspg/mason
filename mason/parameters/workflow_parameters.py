import re
from typing import Optional, List, Tuple, Dict, Any, Union

from mason.engines.scheduler.models.schedule import Schedule, InvalidSchedule

from mason.definitions import from_root
from mason.parameters.input_parameters import InputParameters
from mason.parameters.invalid_parameter import InvalidParameter
from mason.parameters.util import parse_dict
from mason.parameters.workflow_parameter import WorkflowParameter
from mason.util.json_schema import object_from_json_schema
from mason.util.yaml import parse_yaml_invalid

class WorkflowParameters:
    def __init__(self, parameter_path: Optional[str] = None, parameter_dict: Optional[dict] = None):
        parameters: List[WorkflowParameter] = []

        schedule: Optional[str] = None

        if parameter_path:
            parameters, invalid, schedule = self.parse_path(parameter_path or "")
        elif parameter_dict:
            parameters, invalid, schedule = self.parse_param_dict(parameter_dict or {})
        else:
            invalid = [InvalidParameter("No parameter path or json specified")]

        self.schedule = schedule
        self.parameters: List[WorkflowParameter] = parameters
        self.invalid: List[InvalidParameter] = invalid

    def get(self, step_id: str) -> Optional[WorkflowParameter]:
        return next((x for x in self.parameters if x.step == step_id), None)

    def parse_param_dict(self, param_dict: dict) -> Tuple[List[WorkflowParameter], List[InvalidParameter], Optional[str]]:
        valid: List[WorkflowParameter] = []
        invalid: List[InvalidParameter] = []
        schedule: Optional[str] = None
        if isinstance(param_dict, dict):
            validated = object_from_json_schema(param_dict, from_root("/parameters/workflow_schema.json"), dict)
            if isinstance(validated, dict):  #can now be confident it is matches schema definition
                schedule = validated.get("schedule")
                for key, value in validated.items():
                    if key != "schedule":
                        config_id: str = str(value["config_id"])
                        parameters: Dict[str,Dict[str, Any]] = value["parameters"]
                        valid_step, invalid_step = parse_dict(parameters, from_root("/parameters/schema.json"))
                        ip = InputParameters()
                        ip.parameters = valid_step
                        ip.invalid = invalid_step
                        valid.append(WorkflowParameter(key, config_id, ip))
            else:
                invalid.append(InvalidParameter(f"Invalid parameters: {validated.reason}"))
        else:
            invalid.append(InvalidParameter(f"Parameters do not conform to specified schema in parameters/workflow_schema.json.  Must be of form step_id: key:value.  {validated.reason}"))

        return valid, invalid, schedule


    def parse_path(self, param_path: str) -> Tuple[List[WorkflowParameter], List[InvalidParameter], Optional[str]]:
        parsed = parse_yaml_invalid(param_path)
        valid: List[WorkflowParameter] = []
        invalid: List[InvalidParameter] = []
        schedule: Optional[str] = None
        if isinstance(parsed, dict):
            valid, invalid, schedule = self.parse_param_dict(parsed)
        else:
            invalid.append(InvalidParameter(f"Invalid parameter yaml: {parsed}"))

        return valid, invalid, schedule

