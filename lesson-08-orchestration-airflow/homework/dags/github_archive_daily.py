"""github_archive_daily — ВАШ DAG. Специфікація: ../SPEC.md → «DAG».

Готові ETL-цеглинки вже є — імпортуйте і викликайте їх у задачах (не переписуйте):

    from include.gh_etl import download, validate, load_to_duckdb, summarize
    from gh_sensor import GHArchiveSensor   # ваш custom sensor із plugins/

Що треба зібрати (деталі й бали — у SPEC.md):
  * DAG `github_archive_daily`, розклад «щодня о 06:00 UTC», catchup=False;
  * усі задачі працюють із logical date {{ ds }}, а не datetime.now() — це дає
    ідемпотентність і коректний backfill;
  * граф:
        check_availability -> download_archive -> validate_file
            -> load_to_duckdb -> notify_completion
  * download_archive кладе шлях у XCom; validate_file і load_to_duckdb беруть його з XCom;
  * шляхи (дано):
        DB_PATH     = "/opt/airflow/data/github_analytics.duckdb"
        LANDING_DIR = "/opt/airflow/data/landing"

Перевірка: `airflow dags test github_archive_daily 2024-01-14` має пройти всі задачі;
наскрізно — `./verify.sh` із кореня homework/.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from include.gh_etl import download, load_to_duckdb, summarize, validate
from gh_sensor import GHArchiveSensor


DB_PATH = "/opt/airflow/data/github_analytics.duckdb"
LANDING_DIR = "/opt/airflow/data/landing"

def download_archive(ds, **kwargs):
    return download(ds, LANDING_DIR)


def validate_file(ti, **kwargs):
    path = ti.xcom_pull(task_ids="download_archive")
    validate(path)
    return path


def load_events(ti, ds, **kwargs):
    path = ti.xcom_pull(task_ids="download_archive")
    if not path:
        raise ValueError("No path found in XCom for task 'download_archive'")   
    print(f"DEBUG: ds={ds}, path={path}")
    return load_to_duckdb(path, ds, DB_PATH)


def notify_completion(ds, **kwargs):
    result = summarize(ds, DB_PATH)

    print(
        f"Completed {ds}: "
        f"{result['rows']} rows, "
        f"{result['event_types']} event types"
    )


with DAG(
    dag_id="github_archive_daily",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["github", "archive"],
) as dag:

      check_availability = GHArchiveSensor(
          task_id="check_availability",
          hour=14,
          timeout=600,
          poke_interval=60,
          mode="poke",
      )
      

      download_task = PythonOperator(
          task_id="download_archive",
          python_callable=download_archive,
          op_kwargs={
              "ds": "{{ ds }}",
          },
      )

      validate_task = PythonOperator(
          task_id="validate_file",
          python_callable=validate_file,
      )

      load_task = PythonOperator(
          task_id="load_to_duckdb",
          python_callable=load_events,
          op_kwargs={
              "ds": "{{ ds }}",
          },
      )

      notify_task = PythonOperator(
          task_id="notify_completion",
          python_callable=notify_completion,
          op_kwargs={
              "ds": "{{ ds }}",
          },
      )

(
    check_availability
    >> download_task
    >> validate_task
    >> load_task
    >> notify_task
)

