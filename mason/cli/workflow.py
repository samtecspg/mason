from typing import Optional
import click

@click.command("workflow", short_help="Executes and lists mason workflows")
@click.argument("cmd", required=False)
@click.argument("subcmd", required=False)
@click.option("-l", "--log_level", help="Log level for mason")
@click.option("-d", "--deploy", help="Deploy specified workflow", is_flag=True)
@click.option('-r', '--run', help="Run workflow right now ignoring schedule", is_flag=True)
@click.option('-f', '--param_file', help="Parameters from yaml file path. For workflows this is the only way to pass parameters")
@click.option('-n', '--schedule_name', help="Optional name for schedule.  Only works with -d --deploy")
@click.option('-o', '--output_path', help="Optional output path for client DAG definition.  Only works with -d --deploy", required=False)
def workflow(cmd: Optional[str] = None, subcmd: Optional[str] = None, param_file: Optional[str] = None, log_level: Optional[str] = None, deploy: bool = False, run: bool = False, schedule_name: Optional[str] = None, output_path: Optional[str] = None):
    """
    Running without cmd or subcmd will list out all mason workflows currently registered.
    Running without subcmd will list out all mason workflows under the cmd namespace.
    Running with both cmd and subcmd will execute the workflow or print out missing required parameters.
    """
    from mason.configurations.configurations import get_current_config
    from mason.parameters.workflow_parameters import WorkflowParameters
    from mason.util.environment import MasonEnvironment
    from mason.util.logger import logger
    from mason.workflows import workflows

    env = MasonEnvironment()
    logger.set_level(log_level)
    config = get_current_config(env, "debug")

    if config:
        params = WorkflowParameters(param_file)
        workflows.run(env, config, params, cmd, subcmd, deploy, run, schedule_name, output_path)
    else:
        logger.error("Configuration not found.  Run \"mason config\" first")

