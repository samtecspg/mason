
from typing import Optional
from util.environment import MasonEnvironment
from os import path
from clients.response import Response
from configurations import get_all

def list(config_file: Optional[str], log_level: Optional[str]) :
    response = Response()
    env: MasonEnvironment = MasonEnvironment()
    if path.exists(env.config_home):
        config = get_all(env)[0]
        response.add_config(config)


    return response.formatted(), response.status_code
