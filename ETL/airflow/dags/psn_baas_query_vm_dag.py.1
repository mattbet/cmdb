from datetime import datetime
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
import json

# Sostituisci con l'ID della tua connessione PostgreSQL salvata su Airflow
POSTGRES_CONN_ID = 'postgres_airflow' 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
}

def parse_and_generate_sql(response):
    """
    Legge la risposta JSON dell'API, estrae nome e stato,
    e genera una serie di istruzioni SQL INSERT destinate a Postgres.
    """
    baas_data = response.json()
    assets = baas_data.get('items', []) 
    
    if not assets:
        return "SELECT 1;" # Query fittizia se non ci sono asset per evitare errori
        
    sql_statements = []
    for asset in assets:
        # Pulisce i valori per evitare SQL Injection basilari nei campi stringa
        name = asset.get('name', 'N/D').replace("'", "''")
        status = asset.get('status', 'Unknown').replace("'", "''")
        
        sql_statements.append(
            f"INSERT INTO psn_baas_assets (asset_name, backup_status) VALUES ('{name}', '{status}');"
        )
        
    # Unisce tutte le insert in un unico blocco di testo SQL
    return "\n".join(sql_statements)

with DAG(
    dag_id='psn_baas_query_to_postgres',
    default_args=default_args,
    description='Recupera asset da PSN BaaS e salva lo stato su PostgreSQL',
#    schedule_interval='@daily',
    schedule_interval=None,  # Disattiva la pianificazione automatica,
    catchup=False
) as dag:

    # 1. Task iniziale: Richiama il DAG di refresh del token e attende che finisca con successo
    # trigger_token_refresh = TriggerDagRunOperator(
        # task_id='trigger_token_refresh',
        # trigger_dag_id='psn_baas_auth_refresh',  # ID esatto del primo DAG
        # wait_for_completion=True,                 # Forza l'attesa del completamento del refresh
        # poke_interval=10,                         # Controlla lo stato ogni 10 secondi
    # )

    # 2. Interroga l'API BaaS del PSN usando il token appena aggiornato
    query_baas_assets = HttpOperator(
        task_id='query_baas_assets',
        http_conn_id='psn_baas_api',
        endpoint='api/v1/baas/assets',
        method='GET',
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer {{ var.value.psn_baas_token }}"
        },
        response_filter=parse_and_generate_sql, 
    )

    # 3. Esegue le INSERT generate sul database PostgreSQL caricando i dati
    load_to_postgres = SQLExecuteQueryOperator(
        task_id='load_to_postgres',
        conn_id=POSTGRES_CONN_ID,
        sql="{{ task_instance.xcom_pull(task_id='query_baas_assets') }}",
    )

    # Flusso dei task coordinato
    # trigger_token_refresh >> query_baas_assets >> load_to_postgres
    query_baas_assets >> load_to_postgres