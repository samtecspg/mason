import click
import shutil
from operators import Operators
from typing import Optional

from configurations import Config
from parameters import Parameters

@click.group()
def main():
    """
    Mason Data Operator Framework
    """
    pass

@main.command()
@click.argument('config_file')
def config(config_file: str):
    # TODO: Validate config
    print(f"Using config {config_file}")
    shutil.copyfile(config_file, "config.yaml")

@main.command()
@click.argument("cmd", required=False)
@click.argument("subcmd", required=False)
@click.option('-p', '--parameters', help="Parameters as \",\" delimeted list var=value")
@click.option('-c', '--param_file', help="Parameters from yaml file path")
def operator(cmd: Optional[str] = None, subcmd: Optional[str] = None, parameters: Optional[str] = None, param_file: Optional[str] = None):
    params = Parameters(parameters, param_file)
    config = Config()

    Operators().run(config, params, cmd, subcmd)

if __name__ == "__main__":
    main()
