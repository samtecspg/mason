from mason.parameters.operator_parameters import OperatorParameters

class WorkflowParameter:
    def __init__(self, step: str, config_id: str, parameters: OperatorParameters):
        self.config_id = config_id
        self.step = step
        self.parameters = parameters
