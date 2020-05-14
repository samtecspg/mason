
# from flask import session
from typing import Optional

from util.logger import logger
from util.environment import MasonEnvironment
from util.exception import message

CURRENT_CONFIG_FILE = "CURRENT_CONFIG"

def set_session_config(env: MasonEnvironment, config_id: str):
    with open(env.config_home + CURRENT_CONFIG_FILE, 'w+') as f:
        f.write(str(config_id))

    # TODO: Use flask sessions to avoid conflicts of multiple users
    # session['current_config'] = config_id


def get_session_config(env: MasonEnvironment) -> Optional[str]:
    try:
        with open(env.config_home + CURRENT_CONFIG_FILE, 'r') as f:
            return str(f.read())
    except FileNotFoundError as e:
        logger.error("Current Mason config not set.  Run \"mason config -s\" to set current config.")
        return None


