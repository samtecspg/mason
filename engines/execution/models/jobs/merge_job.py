from engines.execution.models.jobs import Job


class MergeJob(Job):

    def __init__(self):
        super().__init__("query")


