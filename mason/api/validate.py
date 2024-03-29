from typing import Optional
from mason.util.environment import MasonEnvironment
from mason.api.run import run

#  Alias for run(dry_run=True)
def validate(resource_type: str, namespace: str, command: str, parameter_string: Optional[str] = None, param_file: Optional[str] = None, config_id: Optional[str] = None, log_level: Optional[str] = None, env: Optional[MasonEnvironment] = None, parameters: Optional[dict] = None):
    return run(resource_type, namespace, command, parameter_string, param_file, config_id, log_level, env, True, parameters)
    

