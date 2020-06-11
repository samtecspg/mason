from mason.api import workflow_api as WorkflowApi
from mason.engines.scheduler.models.dags import DagStep


def api(*args, **kwargs): return WorkflowApi.get("table", "validated_infer", *args, **kwargs)


def step(current: DagStep, next: DagStep):
    pass
    
