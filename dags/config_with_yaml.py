from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

@dag()
def config_with_yaml():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    @task
    def get_config():
        import yaml

        with open("dags/resources/dag-config/data.yaml", "r") as f:
            data = yaml.safe_load(f)

        print("DATA dari config adalah:", data)

    start_task >> get_config() >> end_task

config_with_yaml()


