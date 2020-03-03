import yaml

def parse_yaml(file: str):
    with open(file, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(f"Invalid YAML: {exc}")

