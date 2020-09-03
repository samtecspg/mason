import os
import shutil
from unittest import mock

from mason.test.support.testing_base import clean_string

from mason.clients.response import Response
from mason.configurations.configurations import get_all
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.test.support.mocks import mock_execution_engine_client, mock_storage_engine_client, \
    mock_scheduler_engine_client, mock_metastore_engine_client, mock_config_schema
from mason.workflows import workflows
from mason.definitions import from_root
from mason.test.support import testing_base as base
from mason.workflows.invalid_workflow import InvalidWorkflow
from mason.workflows.valid_workflow import ValidWorkflow


@mock.patch("mason.engines.execution.execution_engine.ExecutionEngine.get_client", mock_execution_engine_client)
@mock.patch("mason.engines.storage.storage_engine.StorageEngine.get_client", mock_storage_engine_client)
@mock.patch("mason.engines.scheduler.scheduler_engine.SchedulerEngine.get_client", mock_scheduler_engine_client)
@mock.patch("mason.engines.metastore.metastore_engine.MetastoreEngine.get_client", mock_metastore_engine_client)
@mock.patch("mason.configurations.config.Config.config_schema", mock_config_schema)
class TestWorkflows:

    def before(self):
        base.set_log_level("fatal")
        mason_home = from_root("/test/.tmp/")

        if os.path.exists(mason_home):
            shutil.rmtree(mason_home)

        env = base.get_env(workflow_home="/test/.tmp/workflows/", operator_home="/test/support/operators/", config_home="/test/support/configs/")
        
        return env, mason_home
    
    def after(self, mason_home: str):
        if os.path.exists(mason_home):
            shutil.rmtree(mason_home)

    def test_workflow_1_valid(self):
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

        if wf:
            params = {
                "step_1": step_params,
                "step_2": step_params,
                "step_3": step_params
            }

            parameters = WorkflowParameters(parameter_dict=params)
            validated = wf.validate(env, config, parameters)
            assert (isinstance(validated, ValidWorkflow))

            run = validated.dry_run(env, Response())
            self.after(mason_home)
        else:
            raise Exception("Workflow not found")

    def test_workflow_1_invalid(self):
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

    def test_workflow_3_valid(self):
        env, mason_home = self.before()
        config = get_all(env)[0]['3']

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

        workflows.register_workflows(from_root("/test/support/workflows/namespace1/workflow3/"), env)
        wf = workflows.get_workflow(env, "namespace1", "workflow3")
        if wf:
            parameters = WorkflowParameters(parameter_dict=params)
            validated = wf.validate(env, config, parameters)

            # step 3 is invalid due to non-existent reference.  This removes the cycle and marks workflow valid.
            # Q:  is this desired?
            assert(isinstance(validated, ValidWorkflow))
        else:
            raise Exception("Workflow not found")


    def test_workflow_2_invalid(self):
        env, mason_home = self.before()
        config = get_all(env)[0]['3']

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
            assert(validated.reason == 'Invalid DAG definition: Invalid Dag: Cycle detected. Repeated steps: step_2 Invalid Dag Steps: Workflow Parameters for step:step_5 not specified. Invalid Parameters: ')
        else:
            raise Exception("Workflow not found")
    

    def test_workflow_4_valid(self):
        env, mason_home = self.before()
        config = get_all(env)[0]['3']
        
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
            "step_4": step_params,
            "step_5": step_params
        }

        workflows.register_workflows(from_root("/test/support/workflows/namespace1/workflow4/"), env)
        wf = workflows.get_workflow(env, "namespace1", "workflow4")
        if wf:
            parameters = WorkflowParameters(parameter_dict=params)
            validated = wf.validate(env, config, parameters)
            assert(isinstance(validated, ValidWorkflow))
            display = """
            *step_1
            | *step_5
            | /
            | *step_4
            | /
            *step_2
            *step_3
            """
            assert(clean_string(validated.dag.display()) == clean_string(display))
        else:
            raise Exception("Workflow not found")

    def test_workflow_6_valid(self):
        env, mason_home = self.before()
        config = get_all(env)[0]['3']
        
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
            "step_4": step_params,
            "step_5": step_params
        }

        workflows.register_workflows(from_root("/test/support/workflows/namespace1/workflow6/"), env)
        wf = workflows.get_workflow(env, "namespace1", "workflow6")
        if wf:
            parameters = WorkflowParameters(parameter_dict=params)
            validated = wf.validate(env, config, parameters)
            assert(isinstance(validated, ValidWorkflow))
            # TODO:  Improve asciidag rendering of this case
            display = """
            * step_1
            | * step_1
            | * step_4
            | * step_5
            |/  
            * step_2
            * step_3
            """
            assert(clean_string(validated.dag.display()) == clean_string(display))
        else:
            raise Exception("Workflow not found")

    def test_workflow_operator_self_dep_invalid(self):
        env, mason_home = self.before()
        config = get_all(env)[0]['3']

        # DAG has step with dependency on itself.
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
            "step_4": step_params,
            "step_5": step_params
        }

        workflows.register_workflows(from_root("/test/support/workflows/namespace1/workflow7/"), env)
        wf = workflows.get_workflow(env, "namespace1", "workflow7")
        if wf:
            parameters = WorkflowParameters(parameter_dict=params)
            validated = wf.validate(env, config, parameters)
            assert(isinstance(validated, InvalidWorkflow))
            assert(validated.reason == "Invalid DAG definition: Invalid Dag: Cycle detected. Repeated steps: step_2 Invalid Dag Steps: ")
        else:
            raise Exception("Workflow not found")

def test_workflow_operator_multiple_roots_valid(self):
        env, mason_home = self.before()
        config = get_all(env)[0]['3']

        # DAG has multiple roots, but is structurally sound.
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
            "step_4": step_params,
            "step_5": step_params
        }

        workflows.register_workflows(from_root("/test/support/workflows/namespace1/workflow8_1/"), env)
        wf = workflows.get_workflow(env, "namespace1", "workflow8_1")
        if wf:
            parameters = WorkflowParameters(parameter_dict=params)
            validated = wf.validate(env, config, parameters)
            assert(isinstance(validated, ValidWorkflow))
            display = """
            * step_1
            * step_4
            | * step_2
            | * step_3
            |/  
            * step_5
            """
            assert(clean_string(validated.dag.display()) == clean_string(display))
        else:
            raise Exception("Workflow not found")

def test_workflow_operator_multiple_roots_cycle_invalid(self):
        env, mason_home = self.before()
        config = get_all(env)[0]['3']

        # DAG has multiple roots, and a cycle occurs downstream of the second root.
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
            "step_4": step_params,
            "step_5": step_params
        }

        workflows.register_workflows(from_root("/test/support/workflows/namespace1/workflow8_2/"), env)
        wf = workflows.get_workflow(env, "namespace1", "workflow8_2")
        if wf:
            parameters = WorkflowParameters(parameter_dict=params)
            validated = wf.validate(env, config, parameters)
            assert(isinstance(validated, InvalidWorkflow))
            assert(validated.reason == "Invalid DAG definition: Invalid Dag: Cycle detected. Repeated steps: step_3 Invalid Dag Steps: ")
        else:
            raise Exception("Workflow not found")

def test_workflow_operator_multiple_roots_forest_valid(self):
        env, mason_home = self.before()
        config = get_all(env)[0]['3']

        # DAG has multiple disconnected roots, but is structurally sound.
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
            "step_4": step_params,
            "step_5": step_params
        }

        workflows.register_workflows(from_root("/test/support/workflows/namespace1/workflow8_3/"), env)
        wf = workflows.get_workflow(env, "namespace1", "workflow8_3")
        if wf:
            parameters = WorkflowParameters(parameter_dict=params)
            validated = wf.validate(env, config, parameters)
            assert(isinstance(validated, ValidWorkflow))
            #TODO: Ensure display string matches good grapher output.
            display = """
            * step_1
            * step_4
            * step_5
            | * step_2
            | * step_3
            """
            assert(clean_string(validated.dag.display()) == clean_string(display))
        else:
            raise Exception("Workflow not found")
