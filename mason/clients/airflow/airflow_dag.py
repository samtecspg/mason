from mason.engines.scheduler.models.dags.client_dag import ClientDag
import json

class AirflowDag(ClientDag):

    def to_json(self) -> str:
        # IMPLEMENT
        return json.dumps({})
