
import click
import yaml

@click.group()
def main():
    """
    Mason Data Operator Framework
    """
    pass

@main.command()
@click.argument('mason_file')
def deploy(mason_file: str):
    parse_yaml(mason_file)

@main.command()
@click.argument('mason_file')
def validate(mason_file: str):
    print(mason_file)


@main.command()
@click.argument('mason_file')
def rehearse(mason_file: str):
    print(mason_file)


def parse_yaml(file: str):
    with open(file, 'r') as stream:
        try:
            print(yaml.safe_load(stream))
        except yaml.YAMLError as exc:
            print(f"Invalid YAML: {exc}")

if __name__ == "__main__":
    main()
