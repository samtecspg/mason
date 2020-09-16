from mason.engines.scheduler.models.dags.client_dag import ClientDag
import json

class AirflowDag(ClientDag):
    def __init__(self, schedule_name: str, mason_dag: ValidDag, schedule: Optional[Schedule]):
        schedule_cron = schedule.definition if schedule is not None else None
        dag_header_config = {
            'owner': "Mason", #TODO: What should go here?
            'schedule_name': schedule_name,
            'schedule_interval': schedule_cron,
            'current_date': "" #TODO: Add current date here.
        }
        dag_file = """
        from airflow import DAG
        from ??? import MasonOperator
        from datetime import datetime

        default_args = {
            'owner': {owner},
            'depends_on_past': False,
            'start_date': datetime({current_date}),
        }

        dag = DAG('{schedule_name}', catchup=False, default_args=default_args, schedule_interval={schedule_interval})
        
        """.format(dag_header_config)
        super().__init__()

    def to_json(self) -> str:
        # IMPLEMENT
        return json.dumps({})
