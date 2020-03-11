import yaml
from util.logger import logger

def parse_yaml(file: str):
    try:
        with open(file, 'r') as stream:
            try:
                return yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                logger.error(f"Invalid YAML: {exc}")
    except FileNotFoundError as e:
        logger.error(f"Specified YAML does not exist: {e}")

