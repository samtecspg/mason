from typing import Optional
import click

class Operator:
    @click.command("operator", short_help="Executes and lists mason operators")
    @click.argument("cmd", required=False)
    @click.argument("subcmd", required=False)
    @click.option('-p', '--parameters', help="Load parameters from mason.parameters string of the format  <param1>:<value1>,<param2>:<value2>")
    @click.option('-f', '--param_file', help="Parameters from yaml file path")
    @click.option("-l", "--log_level", help="Log level for mason")
    def operator(cmd: Optional[str] = None, subcmd: Optional[str] = None, parameters: Optional[str] = None, param_file: Optional[str] = None, log_level: Optional[str] = None):
        """
        Running without cmd or subcmd will list out all mason operators currently registered.
        Running without subcmd will list out all mason operators under the cmd namespace.
        Running with both cmd and subcmd will execute the operator or print out missing required parameters.
        """
        from mason.configurations.configurations import get_current_config
        from mason.operators import operators
        from mason.parameters.input_parameters import InputParameters
        from mason.util.environment import MasonEnvironment
        from mason.util.logger import logger
        
        env = MasonEnvironment()
        config = get_current_config(env, "debug")

        logger.set_level(log_level or "info")

        if config:
            params = InputParameters(parameters, param_file)
            operators.run(env, config, params, cmd, subcmd)
        else:
            logger.info("Configuration not found.  Run \"mason config\" first")

