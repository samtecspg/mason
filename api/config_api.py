
from typing import Optional
from util.environment import initialize_environment, MasonEnvironment
from configurations.actions import run_configuration_actions
from clients.response import Response

def run(env: MasonEnvironment, config_file: Optional[str]=None, set_current: Optional[str]=None, log_level: Optional[str]=None):
    initialize_environment(env)
    return run_configuration_actions(env, config_file=config_file, set_current=set_current, log_level=log_level)

def get(log_level: Optional[str]):
    env = MasonEnvironment()
    response = run(env, log_level=log_level)

    return response.formatted(), response.status_code

def set(set_current: Optional[str], log_level: Optional[str]):
    env = MasonEnvironment()
    response = run(env, set_current=set_current, log_level=log_level)

    return response.formatted(), response.status_code


