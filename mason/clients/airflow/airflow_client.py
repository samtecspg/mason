from typing import Tuple, Optional

from mason.clients.airflow.airflow_dag import AirflowDag
from mason.clients.base import Client
from mason.engines.scheduler.models.dags.valid_dag import ValidDag
from mason.engines.scheduler.models.schedule import Schedule
from mason.util.environment import MasonEnvironment
from mason.clients.response import Response

class AirflowClient(Client):

    def __init__(self, endpoint: str, user: str, password: str):
        self.endpoint = endpoint 
        self.user = user 
        self.password = password 
        
    def to_dict(self) -> dict:
        return {
            'client_name': super().name(),
            'endpoint': self.endpoint,
            'user': self.user,
            'password': "REDACTED"
        }

    def client(self):
        # IMPLEMENT underlying client, likely just requests for API calls: 
        pass

    def register_dag(self, schedule_name: str, valid_dag: ValidDag, schedule: Optional[Schedule], response: Response) -> Tuple[str, Response, Optional[AirflowDag]]:
        
        airflow_dag: Optional[AirflowDag] = self.mason_dag_to_airflow_dag(valid_dag, schedule)
        
        return (schedule_name, response, airflow_dag)
    

    def mason_dag_to_airflow_dag(self, dag: ValidDag, schedule: Optional[Schedule]) -> Optional[AirflowDag]:
        #  IMPLEMENT
        return AirflowDag()

    def trigger_schedule(self, schedule_name: str, response: Response, env: MasonEnvironment) -> Response:
        #  IMPLEMENT :  client POST /api/experimental/dags/<schedule_name>/dag_runs to airflow_response
        # airflow_response = {}
        # if airflow_response.sucess:
        #     response.add_info("Dag ${id} triggered")
        # else:
        #     response.add_error("Dag not found in airflow.  Run 'register_dag' first.")

        return response


