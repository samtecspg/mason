from mason.engines.scheduler.models.dags.client_dag import ClientDag
import json
from datetime import date

class AirflowDag(ClientDag):
    def __init__(self, schedule_name: str, mason_dag: ValidDag, schedule: Optional[Schedule]):
        schedule_cron = schedule.definition if schedule is not None else None
        today = date.today()
        dag_header_config = {
            'owner': "Mason", #TODO: What should go here?
            'schedule_name': schedule_name,
            'schedule_interval': schedule_cron,
            'current_date': "{},{},{}".format(today.year, today.month, today.day)
        }
        self.dag_file = """
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
        task_index = 1
        for dag_step in mason_dag.get_nodes():
            retry_config = "retries={},\nretry_delay=datetime.timedelta(seconds={}),\n".format(dag_step.retry_max, dag_step.retry_delay)
            if dag_step.retry_method is "exponential":
                retry_config += "retry_exponential_backoff=True,\n"
            
            
            self.dag_file += """
            t{i} = MasonOperator(
                task_id = {name},
                operator = {op},
                {retry_config}
                dag = dag
            )
            """.format(i=task_index, name=dag_step.id, op=dag_step.operator, retry_config=retry_config)
            task_index += 1
        super().__init__()

    def to_json(self) -> str:
        # IMPLEMENT
        return json.dumps({})
