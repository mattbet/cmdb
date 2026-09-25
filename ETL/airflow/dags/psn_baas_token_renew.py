import time
from datetime import datetime
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = 'postgres_default'
SERVICE_NAME = 'psn_baas'  # Nome del servizio

def check_token_status():
    """
    Verifica se i token per il servizio BaaS sono presenti e validi.
    """
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    select_query = """
        SELECT access_token, refresh_token, token_expiry_timestamp 
        FROM maelstrom.tab_psn_services 
        WHERE serv_name = %s;
    """
    
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(select_query, (SERVICE_NAME,))
    row = cursor.fetchone()
    cursor.close()
    connection.close()
    
    if not row or not row[0] or not row[1] or not row[2]:
        print(f"Verifica fallita: Token o timestamp mancanti per il servizio {SERVICE_NAME}.")
        return False
        
    access_token, refresh_token, token_expiry = row
    current_timestamp = int(time.time())
    
    # Margine di sicurezza di 5 minuti (300 secondi)
    if current_timestamp >= (int(token_expiry) - 300):
        print(f"Verifica fallita: L'accessToken per {SERVICE_NAME} è scaduto o in scadenza.")
        return False
        
    print(f"I token per {SERVICE_NAME} sono presenti e ancora validi.")
    return True


def renew_and_store_tokens():
    """
    Escale la chiamata API di rinnovo e SOVRASCRIVE il record esistente per BaaS.
    """
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    # Recupera i dati attuali per il servizio BaaS
    select_query = """
        SELECT serv_fqdn, access_token, refresh_token 
        FROM maelstrom.tab_psn_services 
        WHERE serv_name = %s;
    """
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(select_query, (SERVICE_NAME,))
    row = cursor.fetchone()
    cursor.close()
    connection.close()
    
    if not row or not row[0] or not row[1] or not row[2]:
        raise ValueError(f"Impossibile procedere: Record iniziale mancante per il servizio {SERVICE_NAME}.")
        
    serv_fqdn, access_token, refresh_token = row

    # Chiamata API HTTP POST
    url = f"https://{serv_fqdn}/commandcenter/api/v4/AccessToken/Renew"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authtoken": access_token
    }
    payload = {
        "accessToken": access_token,
        "refreshToken": refresh_token
    }
    
    print(f"Invio richiesta di rinnovo per {SERVICE_NAME} a: {url}")
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    
    response_data = response.json()
    new_access_token = response_data.get("accessToken")
    new_refresh_token = response_data.get("refreshToken", refresh_token)
    refresh_token_expiry = response_data.get("refreshTokenExpiryTimestamp")
    token_expiry = response_data.get("tokenExpiryTimestamp")
    
    # Query di UPDATE condizionata su SERVICE_NAME per sovrascrivere i vecchi valori
    update_query = """
        UPDATE maelstrom.tab_psn_services 
        SET 
            access_token = %s, 
            refresh_token = %s, 
            refresh_token_expiry_timestamp = %s, 
            token_expiry_timestamp = %s, 
            updated_at = %s
        WHERE serv_name = %s;
    """
    
    pg_hook.run(
        update_query, 
        parameters=(
            new_access_token, 
            new_refresh_token, 
            refresh_token_expiry, 
            token_expiry, 
            datetime.now(),
            SERVICE_NAME
        )
    )
    print(f"Record {SERVICE_NAME} aggiornato e sovrascritto con successo su maelstrom.tab_psn_services.")
