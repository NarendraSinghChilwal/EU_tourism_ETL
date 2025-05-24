from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.models import Variable

default_args = {
    "owner": "narendra",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="tourism_simple_pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Fetch config from Airflow Variables
    S3_BUCKET = Variable.get("S3_BUCKET", default_var="s3a://s3-mycollege")
    DB_CONFIG = Variable.get("DB_CONFIG", deserialize_json=True)

    shared_params = {
        "S3_BUCKET": S3_BUCKET,
        "DB_CONFIG": DB_CONFIG,
    }

    ingest = PapermillOperator(
        task_id="01_ingest",
        input_nb="notebooks/01_ingest.ipynb",
        output_nb="notebooks/output/01_ingest_{{ ds }}.ipynb",
        parameters={**shared_params, "run_ingest": True},
    )

    agg_capacity = PapermillOperator(
        task_id="02_agg_capacity",
        input_nb="notebooks/02_agg_capacity.ipynb",
        output_nb="notebooks/output/02_agg_capacity_{{ ds }}.ipynb",
        parameters={**shared_params, "run_agg_capacity": True},
    )

    agg_occupancy = PapermillOperator(
        task_id="03_agg_occupancy",
        input_nb="notebooks/03_agg_occupancy.ipynb",
        output_nb="notebooks/output/03_agg_occupancy_{{ ds }}.ipynb",
        parameters={**shared_params, "run_agg_occupancy": True},
    )

    join = PapermillOperator(
        task_id="04_join",
        input_nb="notebooks/04_join.ipynb",
        output_nb="notebooks/output/04_join_{{ ds }}.ipynb",
        parameters={**shared_params, "run_join": True},
    )

    write_to_sql = PapermillOperator(
        task_id="05_write_to_sql",
        input_nb="notebooks/05_write_to_sql.ipynb",
        output_nb="notebooks/output/05_write_to_sql_{{ ds }}.ipynb",
        parameters={**shared_params, "write_to_sql": True},
    )

    read_from_sql = PapermillOperator(
        task_id="06_read_from_sql",
        input_nb="notebooks/06_read_from_sql.ipynb",
        output_nb="notebooks/output/06_read_from_sql_{{ ds }}.ipynb",
        parameters={**shared_params, "read_from_sql": True},
    )

    model = PapermillOperator(
        task_id="07_model",
        input_nb="notebooks/07_model.ipynb",
        output_nb="notebooks/output/07_model_{{ ds }}.ipynb",
        parameters={**shared_params, "run_model": True},
    )

    cluster = PapermillOperator(
        task_id="08_cluster",
        input_nb="notebooks/08_cluster.ipynb",
        output_nb="notebooks/output/08_cluster_{{ ds }}.ipynb",
        parameters={**shared_params},
    )

    map_viz = PapermillOperator(
        task_id="09_map",
        input_nb="notebooks/09_map.ipynb",
        output_nb="notebooks/output/09_map_{{ ds }}.ipynb",
        parameters={**shared_params},
    )

    # Define dependencies
    ingest >> [agg_capacity, agg_occupancy] >> join
    join >> write_to_sql >> read_from_sql >> model >> cluster >> map_viz
