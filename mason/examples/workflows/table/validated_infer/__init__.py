from mason.api import workflow_api as WorkflowApi
from mason.engines.scheduler.models.dags import DagStep


def api(*args, **kwargs): return WorkflowApi.get("table", "validated_infer", *args, **kwargs)


def step(current: DagStep, next: DagStep):
    if current.id == "step_1" and next.id == "step_2":
        pass
    elif current.id == "step_2" and next.id == "step_3":
        pass
    elif current.id == "step_3" and next.id == "step_4":
        pass
    else:
        pass
