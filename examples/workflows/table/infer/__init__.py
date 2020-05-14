
from api import workflow_api as WorkflowApi

def api(*args, **kwargs): return WorkflowApi.get("table", "infer", *args, **kwargs)

