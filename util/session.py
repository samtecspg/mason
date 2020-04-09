
# from flask import session # type: ignore
from util.logger import logger
from util.environment import MasonEnvironment
from util.exception import message

CURRENT_CONFIG_FILE = "CURRENT_CONFIG"

def set_session_config(env: MasonEnvironment, config_id: int):
    with open(env.config_home + CURRENT_CONFIG_FILE, 'w+') as f:
        f.write(str(config_id))

    # TODO: Use flask sessions to avoid conflicts of multiple users
    # session['current_config'] = config_id


def get_session_config(env: MasonEnvironment) -> int:
    try:
        with open(env.config_home + CURRENT_CONFIG_FILE, 'r') as f:
            config_id = int(f.read())
    except Exception as e:
        logger.error(message(e))
        config_id = 0

    return config_id

