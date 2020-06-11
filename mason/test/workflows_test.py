import os
import shutil
from unittest import mock

from mason.configurations.configurations import get_all, get_config
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.test.support.mocks import mock_execution_engine_client, mock_storage_engine_client, \
    mock_scheduler_engine_client, mock_metastore_engine_client, mock_config_schema
from mason.workflows import workflows
from mason.definitions import from_root
from mason.test.support import testing_base as base
from mason.util.logger import logger
from mason.workflows.invalid_workflow import InvalidWorkflow
from mason.workflows.valid_workflow import ValidWorkflow


@mock.patch("mason.engines.execution.execution_engine.ExecutionEngine.get_client", mock_execution_engine_client)
@mock.patch("mason.engines.storage.storage_engine.StorageEngine.get_client", mock_storage_engine_client)
@mock.patch("mason.engines.scheduler.scheduler_engine.SchedulerEngine.get_client", mock_scheduler_engine_client)
@mock.patch("mason.engines.metastore.metastore_engine.MetastoreEngine.get_client", mock_metastore_engine_client)
@mock.patch("mason.configurations.config.Config.config_schema", mock_config_schema)
class TestWorkflows:

    def before(self):
        base.set_log_level("trace")
        mason_home = from_root("/test/.tmp/")

        if os.path.exists(mason_home):
            shutil.rmtree(mason_home)

        env = base.get_env(workflow_home="/test/.tmp/workflows/", operator_home="/test/support/operators/", config_home="/test/support/configs/")
        
        return env, mason_home
    
    def after(self, mason_home: str):
        if os.path.exists(mason_home):
            shutil.rmtree(mason_home)

    def test_workflow_1(self):
        env, mason_home = self.before()
        
        workflows.register_workflows(from_root("/test/support/workflows/"), env)
        wf = workflows.get_workflow(env, "namespace1", "workflow1")
        config = get_all(env)[0]['3']

        step_params = {
            "config_id": "5",
            "parameters": {
                "test_param": "test"
            }
        }
        
        # Broken dag, unreachable nodes
        params = {  "step_2": step_params }
        parameters = WorkflowParameters(parameter_dict=params)
        if wf:
            validated = wf.validate(env, config, parameters)
            assert(isinstance(validated, InvalidWorkflow))
            assert("Invalid DAG definition: Invalid Dag: Unreachable steps:"  in validated.reason)

            params = {
                "step_1": step_params,
                "step_2": step_params,
                "step_3": step_params
            }
            parameters = WorkflowParameters(parameter_dict=params)
            validated = wf.validate(env, config, parameters)
            assert(isinstance(validated, ValidWorkflow))

            self.after(mason_home)
        else:
            raise Exception("Workflow not found")


            
    def test_workflow_2(self):
        env, mason_home = self.before()
        config = get_all(env)[0]['3']

        # DAG has cycle
        step_params = {
            "config_id": "5",
            "parameters": {
                "test_param": "test"
            }
        }
        params = {
            "step_1": step_params,
            "step_2": step_params,
            "step_3": step_params,
            "step_4": step_params
        }
        
        workflows.register_workflows(from_root("/test/support/workflows/namespace1/workflow2/"), env)
        wf = workflows.get_workflow(env, "namespace1", "workflow2")
        if wf:
            parameters = WorkflowParameters(parameter_dict=params)
            validated = wf.validate(env, config, parameters)

            assert(isinstance(validated, InvalidWorkflow))
            assert("Invalid DAG definition: Invalid Dag:  Cycle detected. Repeated steps: {'step_2'}" in validated.reason)
        else:
            raise Exception("Workflow not found")
