from datetime import datetime, timedelta
import base64
import requests
from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

# ==========================================
# FUNZIONE DI SINCRONIZZAZIONE
# ==========================================

def fetch_and_store_commvault_assets():
    # 1. Recupero credenziali Commvault tramite HttpHook
    cv_hook = HttpHook(http_conn_id='BaaS_Nord', method='POST')
    cv_conn = cv_hook.get_connection('BaaS_Nord')
    
    cv_host = cv_conn.host
    cv_user = cv_conn.login
    cv_pass = cv_conn.password

    # Controllo di sicurezza per intercettare il campo vuoto
    if not cv_pass:
        raise ValueError("ERRORE: La password nella connessione 'BaaS_Nord' risulta vuota. Verificala nella Web UI di Airflow.")

    # Codifica obbligatoria della password in Base64 per Commvault
    password_bytes = cv_pass.encode('utf-8')
#    password_base64 = base64.b64encode(password_bytes).decode('utf-8')
    password_base64 = "Z084eDdMSGldam5LLTdO"
    
    # 2. Autenticazione Commvault
    login_url = f"https://{cv_host}/webconsole/api/Login"
    login_payload = {"username": cv_user, "password": password_base64}
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    
    response = requests.post(login_url, json=login_payload, headers=headers, verify=False)
    response.raise_for_status()
    
    login_result = response.json()
    token = login_result.get("token")
    
    # Debug in caso di credenziali errate o account bloccato
    if not token:
        raise ValueError(f"Impossibile ottenere il token di autenticazione. Risposta da Commvault: {login_result}")

    
    # 3. Estrazione Asset e Stato di Protezione
    client_url = f"https://{cv_host}/webconsole/api/Client"
    auth_headers = {"Accept": "application/json", "Authtoken": token}
    
    client_response = requests.get(client_url, headers=auth_headers, verify=False)
    client_response.raise_for_status()
    clients_data = client_response.json().get("clientProperties", [])
    
    parsed_assets = []
    current_time = datetime.now()
    
    for client in clients_data:
        client_info = client.get("client", {})
        client_id = client_info.get("clientId")
        client_name = client_info.get("clientName")
        is_protected = client.get("clientIncommCell", {}).get("enableBackup", True) 
        
        parsed_assets.append((client_id, client_name, is_protected, current_time))
        
    # 4. Scrittura su PostgreSQL tramite PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='postgres_airflow')
    
    # Creazione della tabella se non esiste
    create_table_query = """
        CREATE TABLE IF NOT EXISTS BaaS_Nord_asset_status (
            client_id INT PRIMARY KEY,
            client_name VARCHAR(255),
            is_protected BOOLEAN,
            last_checked TIMESTAMP
        );
    """
    pg_hook.run(create_table_query)
    
    # Inserimento o aggiornamento (UPSERT)
    upsert_query = """
        INSERT INTO commvault_asset_status (client_id, client_name, is_protected, last_checked)
        VALUES %s
        ON CONFLICT (client_id) 
        DO UPDATE SET 
            client_name = EXCLUDED.client_name,
            is_protected = EXCLUDED.is_protected,
            last_checked = EXCLUDED.last_checked;
    """
    
    # Il metodo insert_rows gestisce autonomamente l'esecuzione bulk in sicurezza
    pg_hook.insert_rows(
        table='commvault_asset_status',
        rows=parsed_assets,
        target_fields=['client_id', 'client_name', 'is_protected', 'last_checked'],
        replace=True,
        replace_index='client_id'
    )
    
    print(f"Sincronizzazione completata con successo per {len(parsed_assets)} asset.")

# ==========================================
# DEFINIZIONE DELLA DAG
# ==========================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='commvault_secure_asset_sync',
    default_args=default_args,
    description='Estrazione sicura asset Commvault a PostgreSQL usando Airflow Connections',
#    schedule_interval='@daily',
        schedule_interval=None,  # Disattiva la pianificazione automatica,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['commvault', 'postgres', 'security', 'production'],
) as dag:

    sync_task = PythonOperator(
        task_id='fetch_and_store_assets_securely',
        python_callable=fetch_and_store_commvault_assets,
    )

    sync_task
