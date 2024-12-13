## Laporan Assignment Airflow: ETL Assignment

### 1. Pendahuluan

Pada tugas ini, saya telah mengimplementasikan proses ETL (Extract, Transform, Load) menggunakan Apache Airflow. Tugas ini mencakup pembuatan pipeline ETL sederhana yang:

Mengekstrak data dari berbagai sumber (CSV atau JSON).

Menyimpan data dalam format staging area (Parquet).

Memuat data hasil ekstraksi ke database SQLite.

Pipeline ini dirancang menggunakan konsep DAG (Directed Acyclic Graph) untuk memastikan proses dapat berjalan secara otomatis dan terstruktur.



### 2. Tujuan

Memahami bagaimana membangun pipeline ETL menggunakan Apache Airflow.

Mengimplementasikan BranchOperator untuk memilih task berdasarkan parameter yang diberikan.

Menyimpan data hasil ekstraksi ke staging area dan memuatnya ke database SQLite.

Menerapkan konsep trigger rule untuk mengatur eksekusi task akhir meskipun beberapa task di-skip.



### 3. Teknologi yang Digunakan

Apache Airflow: Untuk mendefinisikan dan menjalankan pipeline ETL.

SQLite: Sebagai database untuk menyimpan hasil data.

Docker: Untuk mengelola environment yang diperlukan.

Python: Untuk menulis task dalam pipeline.



### 4. Implementasi

#### 4.1 Struktur DAG

Pipeline yang dibuat memiliki struktur DAG sebagai berikut:

start_task: Task awal untuk memulai eksekusi pipeline.

choose_extract_task: BranchOperator untuk memilih jalur ekstraksi berdasarkan parameter source_type (CSV atau JSON).

extract_from_csv: Task untuk mengekstrak data dari file CSV.

extract_from_json: Task untuk mengekstrak data dari file JSON.

load_to_sqlite: Task untuk memuat data dari staging area ke database SQLite.

end_task: Task akhir untuk menandai bahwa pipeline selesai.

#### 4.2 Penjelasan Logika DAG

BranchOperator (choose_extract_task) digunakan untuk memilih task ekstraksi yang akan dijalankan (extract_from_csv atau extract_from_json).

Data hasil ekstraksi disimpan dalam format Parquet di folder staging area.

Task load_to_sqlite memuat data dari staging area ke tabel SQLite.

Task end_task menggunakan TriggerRule.NONE_FAILED untuk memastikan eksekusi meskipun ada task yang di-skip.

#### 4.3 Kode Implementasi

Kode implementasi DAG adalah sebagai berikut:

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









### 

### 

### 

### 

### 

### 5. Hasil Eksekusi

#### 5.1 Screenshot DAG

Berikut adalah screenshot dari tampilan DAG di UI Airflow:



Parameter



#### 5.2 Screenshot SQLite

Berikut adalah screenshot dari tabel hasil di SQLite setelah data berhasil dimuat:



#### 5.3 Log Eksekusi

Eksekusi pipeline berhasil dilakukan dengan log sebagai berikut:

Task extract_from_csv berhasil dijalankan.

Task load_to_sqlite berhasil memuat data ke SQLite.

Task end_task berhasil dijalankan meskipun task extract_from_json di-skip.



### 6. Kesimpulan

Tugas ini memberikan pemahaman mendalam tentang bagaimana:

Menggunakan Apache Airflow untuk membangun pipeline ETL.

Menerapkan BranchOperator untuk pengambilan keputusan dinamis dalam pipeline.

Menyimpan data hasil ekstraksi dalam staging area dengan format Parquet.

Memastikan task akhir berjalan meskipun beberapa task di-skip menggunakan trigger rule.



