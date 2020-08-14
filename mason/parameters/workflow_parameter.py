from mason.parameters.input_parameters import InputParameters

class WorkflowParameter:
    def __init__(self, step: str, config_id: str, parameters: InputParameters):
        self.config_id = config_id
        self.step = step
        self.parameters = parameters
