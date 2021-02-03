from typing import Optional

from mason.util.environment import MasonEnvironment
from mason.util.logger import logger

CURRENT_CONFIG_FILE = "CURRENT_CONFIG"

def set_session_config(env: MasonEnvironment, config_id: str):
    return None
    #  with open(env.config_home + CURRENT_CONFIG_FILE, 'w+') as f:
        #  f.write(str(config_id))

    # TODO: Use flask sessions to avoid conflicts of multiple users
    # session['current_config'] = config_id


def get_session_config(env: MasonEnvironment) -> Optional[str]:
    return None
    #  try:
    #  with open(env.config_home + CURRENT_CONFIG_FILE, 'r') as f:
        #  return str(f.read())
#  except FileNotFoundError as e:
    #  logger.error("Current Mason config not set.  Run \"mason config -s\" to set current config.")
    #  return None


