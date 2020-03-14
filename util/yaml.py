import yaml
from util.logger import logger

def parse_yaml(file: str):
    try:
        with open(file, 'r') as stream:
            try:
                yaml_load: dict =  yaml.safe_load(stream)
                if type(yaml_load).__name__ == "dict":
                    return yaml_load
                else:
                    logger.error(f"\nInvalid YAML: {yaml_load}\n")
            except yaml.YAMLError as exc:
                logger.error(f"\nInvalid YAML: {exc}\n")
    except FileNotFoundError as e:
        logger.error(f"Specified YAML does not exist: {e}")

