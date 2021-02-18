from typing import Union

from mason.clients.response import Response
from mason.configurations.config import Config
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.test.support.testing_base import get_env, clean_string
from mason.util.environment import MasonEnvironment
from mason.util.logger import logger
from mason.workflows.invalid_workflow import InvalidWorkflow
from mason.workflows.valid_workflow import ValidWorkflow
from mason.workflows.workflow import Workflow
from mason.resources import base

class TestWorkflows:
    def validate_workflow(self, env: MasonEnvironment, command: str, config_id: str, params: dict) -> Union[ValidWorkflow, InvalidWorkflow]:
        logger.set_level("fatal")
        res = base.Resources(env)
        wf = res.get_workflow("testing_namespace", command)
        config = res.get_config(config_id)
        if wf and config and isinstance(wf, Workflow) and isinstance(config, Config):
            print("HERE")
            parameters = WorkflowParameters(parameter_dict=params)
            validated = wf.validate(env, config, parameters)
            return validated
        else:
            raise Exception("Invalid workflow or config")

        
    def test_workflow_basic_valid(self):
        env = get_env("/test/support/", "/test/support/validations/")
        step_params = {
            "config_id": "5",
            "parameters": {
                "test_param": "test"
            }
        }
        params = {
            "step_1": step_params,
            "step_2": step_params,
            "step_3": step_params
        }
        expects = [
            'Performing Dry Run for Workflow',
            '', 
            'Valid Workflow DAG Definition:',
            '--------------------------------------------------------------------------------',
            '* step_1\n* step_2\n* step_3\n', ''
        ]
        
        validated = self.validate_workflow(env, "workflow_basic", '3', params)
        run = validated.dry_run(env, Response())
        assert(run.response.info == expects)

    def test_workflow_step_params_invalid(self):
        env = get_env("/test/support/", "/test/support/validations/")
        step_params = {
            "config_id": "5",
            "parameters": {
                "test_param": "test"
            }
        }

        # Broken dag, unreachable nodes
        # With strict DAG validation setting, throws an error earlier in validation.
        params = { "step_2": step_params }
        validated = self.validate_workflow(env, "workflow_basic", '3', params)
        
        assert(isinstance(validated, InvalidWorkflow))
        # assert("Invalid DAG definition: Invalid Dag: Unreachable steps:"  in validated.reason)
        # What's `Invalid Parameters: ` indicating? It's folllowed only by a blank for both of these.
        assert("Workflow Parameters for step:step_1 not specified. Invalid Parameters: " in validated.reason)
        assert("Workflow Parameters for step:step_3 not specified. Invalid Parameters: " in validated.reason)

        params = {
            "step_1": step_params,
            "step_2": step_params,
            "step_3": step_params
        }
        validated = self.validate_workflow(env, "workflow_basic", '3', params)
        
        assert(isinstance(validated, ValidWorkflow))

    def test_workflow_nonexistent_step_and_cycle_invalid(self):
        env = get_env("/test/support/", "/test/support/validations/")
        step_params = {
            "config_id": "5",
            "parameters": {
                "test_param": "test"
            }
        }
        params = {
            "strict": True,
            "step_1": step_params,
            "step_2": step_params,
            "step_3": step_params,
            "step_4": step_params
        }
        validated = self.validate_workflow(env, 'workflow_nonexistent_step_and_cycle', '3', params)

        assert(isinstance(validated, InvalidWorkflow))
        assert("Invalid DAG definition: Invalid DAG, contains invalid steps." in validated.reason)

        params["strict"] = False
        validated = self.validate_workflow(env, 'workflow_nonexistent_step_and_cycle', '3', params)
        assert(isinstance(validated, InvalidWorkflow))
        assert("Invalid DAG definition: Invalid Dag: Unreachable steps: [\'step_4\'] Invalid Dag Steps: Undefined dependent steps:" in validated.reason)

    def test_workflow_cycle_invalid(self):
        env = get_env("/test/support/", "/test/support/validations/")
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
        validated = self.validate_workflow(env, "workflow_cycle", "3", params)
        assert(isinstance(validated, InvalidWorkflow))
        assert(validated.reason == 'Invalid DAG definition: Invalid Dag: Cycle detected. Repeated steps: step_2 Invalid Dag Steps: ')

    def test_workflow_with_multiple_roots_valid(self):
        env = get_env("/test/support/", "/test/support/validations/")
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

        validated = self.validate_workflow(env, "workflow_multiple_roots", '3', params)
        assert(isinstance(validated, ValidWorkflow))
        display = """
        * step_1
        | * step_4
        |/  
        | * step_5
        |/  
        * step_2
        * step_3
        """
        assert(clean_string(validated.dag.display()) == clean_string(display))

    def test_workflow_parallel_without_cycles_valid(self):
        env = get_env("/test/support/", "/test/support/validations/")
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

        validated = self.validate_workflow(env, 'workflow_parallel_without_cycles', '3', params)
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

    def test_workflow_operator_self_dependency_invalid(self):
        env = get_env("/test/support/", "/test/support/validations/")
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

        validated = self.validate_workflow(env, "workflow_operator_self_dependency", '3', params)
        assert(isinstance(validated, InvalidWorkflow))
        assert(validated.reason == "Invalid DAG definition: Invalid Dag: Cycle detected. Repeated steps: step_2 Invalid Dag Steps: ")

    def test_workflow_with_multiple_roots_valid_2(self):
        env = get_env("/test/support/", "/test/support/validations/")

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
        validated = self.validate_workflow(env, 'workflow_multiple_roots_2', '3', params)
        assert(isinstance(validated, ValidWorkflow))
        display = """
        * step_1
        | * step_2
        * | step_4
        | * step_3
        |/  
        * step_5
        """
        assert(clean_string(validated.dag.display()) == clean_string(display))

    def test_workflow_multiple_roots_cycle_invalid(self):
        env = get_env("/test/support/", "/test/support/validations/")

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
        
        validated = self.validate_workflow(env, 'workflow_multiple_roots_cycle', '3', params)
        assert(isinstance(validated, InvalidWorkflow))
        assert(validated.reason == "Invalid DAG definition: Invalid Dag: Cycle detected. Repeated steps: step_3 Invalid Dag Steps: ")

    def test_workflow_multiple_roots_forest_valid(self):
        env = get_env("/test/support/", "/test/support/validations/")

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
        validated = self.validate_workflow(env, 'workflow_multiple_roots_forest', '3', params)

        assert(isinstance(validated, ValidWorkflow))
        display = """
        * step_1
        | * step_2
        * | step_4
        | * step_3
        * step_5
        """
        assert(clean_string(validated.dag.display()) == clean_string(display))
        assert(len(validated.dag.valid_steps) == 5)
