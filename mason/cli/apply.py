import click
from typing import Optional

@click.command("apply", short_help="Apply mason yaml file(s)")
@click.argument('file')
@click.option("-o", "--overwrite", is_flag=True, help="Overwrite existing resources if already exists")
@click.option("-l", "--log_level", help="Log level for mason")
def apply(file: str, overwrite: bool = False, log_level: Optional[str] = None):
    """
    Applies mason yaml (Config, Operator, Workflow)
    Coming soon:  WorkflowRun, OperatorRun, OperatorDryRun, WorkflowDryRun

    [FILE] is a yaml file or directory containing multiple yaml files.  See examples/ for reference implementations.
    """
    from mason.api.apply import apply as api_apply
    api_apply(file, overwrite, log_level)



