import os
import logging
from datetime import datetime, timedelta

from airflow import models
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator

# ─── Logger ───────────────────────────────────────────────────────────────────
log = logging.getLogger(__name__)

# ─── Obtención de Variables desde Airflow UI ──────────────────────────────────

PROJECT_ID          = Variable.get('ENV_PROJECT_ID', default_var=os.environ.get('ENV_PROJECT_ID', 'pub-sub-data-flow'))
LOCATION            = Variable.get('ENV_LOCATION', default_var=os.environ.get('ENV_LOCATION', 'us-central1'))
SERVICE_ACCOUNT     = Variable.get('ENV_SERVICE_ACCOUNT', default_var=os.environ.get('ENV_SERVICE_ACCOUNT', 'pub-sub-data-flow@pub-sub-data-flow.iam.gserviceaccount.com'))
SUBNETWORK          = Variable.get('ENV_SUBNETWORK', default_var=os.environ.get('ENV_SUBNETWORK', ''))
GCS_TEMP_LOCATION   = Variable.get('ENV_GCS_TEMP_LOCATION', default_var=os.environ.get('ENV_GCS_TEMP_LOCATION', 'gs://dataflow-staging-us-central1-51051980873/temp'))

# ─── Conexión SQL Server ──────────────────────────────────────────────────────
AIRFLOW_CONN_ID     = Variable.get('ENV_AIRFLOW_CONN_ID_MSSQL_CONT', default_var='sql_server_retail_conn')
TABLE_NAME          = Variable.get('ENV_TABLE_CLIENTES', default_var='RetailDB.dbo.clientes')

# Solo si hay labels definidos en Airflow, de lo contrario comentar la línea
# LABELS = {
#     "dueno": os.environ.get('LABEL_DUENO'),
#     "aplicacion": "medallion-gcp",
#     "centrocosto": os.environ.get('LABEL_CENTROCOSTO'),
#     "entorno": os.environ.get('ENV_ENVIRONMENT'),
#     "horario": "24x5",
#     "nombre": PROJECT_ID,
#     "proyecto": PROJECT_ID,
#     "servicio": "dataflow"
# }

# ─── Conexión SQL Server via BaseHook ─────────────────────────────────────────
conn        = BaseHook.get_connection(AIRFLOW_CONN_ID)
user_db     = conn.login
password_db = conn.password
database    = conn.schema

# Sobrescribimos host y puerto para Dataflow usando el túnel de Pinggy
dataflow_host = 'pprtp-201-241-207-198.run.pinggy-free.link'
dataflow_port = '36755'

# JDBC URL para Dataflow en GCP
jdbc_url = f"jdbc:sqlserver://{dataflow_host}:{dataflow_port};databaseName={database};"

# ─── Query base SQL Server ───────────────────────────────────────────────────
_raw_sql = f"""
    SELECT
        CAST(CLIENTE_ID AS BIGINT) AS CLIENTE_ID,
        CAST(NOMBRE AS VARCHAR(4000)) AS NOMBRE,
        CAST(EMAIL AS VARCHAR(4000)) AS EMAIL,
        FORMAT(FECHA_REGISTRO, 'yyyy-MM-dd HH:mm:ss') AS FECHA_REGISTRO,
        CAST(FORMAT(SYSDATETIMEOFFSET(), 'yyyy-MM-ddTHH:mm:ss.ffffffzzz') AS VARCHAR(4000)) AS load_timestamp_cl
    FROM {TABLE_NAME}
"""
sql = " ".join(_raw_sql.split())

# ─── DAG ──────────────────────────────────────────────────────────────────────
with models.DAG(
    dag_id="dataflow_flex_sqlserver_retail_clientes",
    #schedule_interval="0 11 * * *",
    #airflow 3.0
    schedule=None,  # <--- Cambio aquí
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=300)
    },
    tags=["bigquery", "dataflow", "sqlserver", "incremental"]
) as dag:
    
    # ─── DELETE Bronze y Silver ───────────────────────────────────────────────
    delete_bronze_silver = BigQueryInsertJobOperator(
        task_id='delete_bronze_and_silver',
        gcp_conn_id='gcp_bigquery_conn',
        configuration={
            'query': {
                'query': f"""
                    -- 1. Delete Bronze
                    TRUNCATE TABLE `{PROJECT_ID}.bronze_retail.clientes`;
                   

                    -- 2. Delete Silver
                    TRUNCATE TABLE `{PROJECT_ID}.silver_retail.clientes`;
                    
                """,
                'useLegacySql': False,
            }
        },
        location=LOCATION,
        project_id=PROJECT_ID
    )

    # ─── Lanzar Dataflow ──────────────────────────────────────────────────────
    start_flex_template_job = DataflowStartFlexTemplateOperator(
        task_id="start_sqlserver_to_bq_flex_job_incremental",
        gcp_conn_id='gcp_bigquery_conn',
        project_id=PROJECT_ID,
        location=LOCATION,
        body={
            "launchParameter": {
                "jobName": f"extract-sqlserver-to-bq-incremental-{os.urandom(4).hex()}",
                "containerSpecGcsPath": "gs://dataflow-templates-us-central1/latest/flex/SQLServer_to_BigQuery",
                "parameters": {
                    "connectionURL": jdbc_url,
                    "username": user_db,
                    "password": password_db,
                    "query": sql,
                    "outputTable": f"{PROJECT_ID}:bronze_retail.clientes",
                    "bigQueryLoadingTemporaryDirectory": GCS_TEMP_LOCATION,
                    "connectionProperties": "integratedSecurity=false;encrypt=true;trustServerCertificate=true",
                },
                "environment": {
                    #"zone": "us-central1-f",  # <--- Probamos con la zona f
                    "machineType": "e2-medium",         # <--- Para el Launcher VM
                    "numWorkers": 1,
                    "maxWorkers": 3,
                    # comentar workerRegion para que tome la zona asignada en zone
                    "workerRegion": LOCATION,
                    "subnetwork": SUBNETWORK,
                    "serviceAccountEmail": SERVICE_ACCOUNT,
                    "additionalExperiments": [
                        "use_runner_v2",
                        "enable_lineage=true"
                        # Asigna la etiqueta de red 'inet-tmp' a las VMs de Dataflow para cumplir con reglas de firewall o rutas de salida
                        #"use_network_tags=inet-tmp"
                        # Propaga la etiqueta de red 'inet-tmp' específicamente a los contenedores y workers efímeros de las Flex Templates
                        #"use_network_tags_for_flex_templates=inet-tmp",
                    ],
                    # Solo si hay labels definidos en Airflow, de lo contrario comentar la línea
                    #"additionalUserLabels": LABELS,
                    
                    # Pinggy crea un puente seguro entre tu computador local y internet. si Dataflow intenta conectarse directamente a tu base de datos local, la nube de Google simplemente "no la ve" porque hay un firewall de por medio
                    # WORKER_IP_PUBLIC pero con piggy, en entorno de GCP en Managed Airflow es WORKER_IP_PRIVATE
                    "ipConfiguration": "WORKER_IP_PUBLIC",
                    "tempLocation": GCS_TEMP_LOCATION
                }
            }
        }
    )

    # ─── Llamar SP en BigQuery ────────────────────────────────────────────────
    bigquery_sp_stage_silver = BigQueryInsertJobOperator(
        task_id='bigquery_sp_stage_silver',
        gcp_conn_id='gcp_bigquery_conn',
        configuration={
            'query': {
                'query': f'CALL `{PROJECT_ID}.bronze_retail.sp_retail_clientes`();',
                'useLegacySql': False,
            }
        },
        location=LOCATION,
        project_id=PROJECT_ID
    )

    # ─── Flujo ────────────────────────────────────────────────────────────────
    delete_bronze_silver >> start_flex_template_job >> bigquery_sp_stage_silver