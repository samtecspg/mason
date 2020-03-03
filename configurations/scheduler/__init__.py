from configurations.scheduler.glue.client import GlueSchedulerClient
from typing import Optional

class SchedulerConfig(object):

    def __init__(self, scheduler_config: dict):
        client: Optional[str] = scheduler_config.get("client")
        args: dict = scheduler_config.get("configuration", {})

        #  TODO: Automate
        if client == "glue":
            aws_role_arn: Optional[str] = args.get("aws_role_arn", None)
            self.client = GlueSchedulerClient(aws_role_arn)  # TODO: Validate glue client schema

        self.client_name = client

    def to_dict(self):
        return {
            'client': self.client_name,
            'configuration': self.client.__dict__
        }


