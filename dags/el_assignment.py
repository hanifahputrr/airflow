from airflow.decorators import dag, task, branch_task
from airflow.operators.empty import EmptyOperator
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import pandas as pd
import sqlite3


@dag(
    schedule_interval=None,
    start_date=datetime(2024, 7, 1),
    catchup=False,
    params={
        "source_type": Param("csv", description="Tipe sumber data: csv atau json"),
        "source_path": Param("https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv", description="URL sumber data"),
    },
    tags=["etl", "assignment"]
)
def el_assignment():

    # Task awal
    start_task = EmptyOperator(task_id="start_task")

    # Task akhir dengan trigger_rule
    end_task = EmptyOperator(
        task_id="end_task",
        trigger_rule=TriggerRule.NONE_FAILED  # Task akan dijalankan jika tidak ada task gagal
    )

    # Branch Operator
    @branch_task()
    def choose_extract_task(params):
        source_type = params["source_type"]
        if source_type == "csv":
            return "extract_from_csv"
        elif source_type == "json":
            return "extract_from_json"
        else:
            raise ValueError("Tipe sumber data tidak valid")

    # Task untuk ekstraksi dari CSV
    @task()
    def extract_from_csv(params):
        url = params["source_path"]
        data = pd.read_csv(url)
        file_path = "/opt/airflow/data/data_staged.parquet"
        data.to_parquet(file_path, index=False)
        return file_path

    # Task untuk ekstraksi dari JSON
    @task()
    def extract_from_json(params):
        url = params["source_path"]
        data = pd.read_json(url)
        file_path = "/opt/airflow/data/data_staged.parquet"
        data.to_parquet(file_path, index=False)
        return file_path

    # Task untuk memuat data ke SQLite
    @task()
    def load_to_sqlite(file_path):
        conn = sqlite3.connect("/opt/airflow/data/assignment.db")
        data = pd.read_parquet(file_path)
        data.to_sql("assignment_table", conn, if_exists="replace", index=False)
        conn.close()
        print(f"Data berhasil dimuat ke SQLite dari file: {file_path}")

    # Definisikan alur task
    choose_task = choose_extract_task()
    csv_task = extract_from_csv()
    json_task = extract_from_json()

    # Hubungkan task
    start_task >> choose_task
    choose_task >> csv_task >> load_to_sqlite(csv_task)
    choose_task >> json_task >> load_to_sqlite(json_task)
    load_to_sqlite(csv_task) >> end_task
    load_to_sqlite(json_task) >> end_task


el_assignment()
