# psn_vcloud_token_renew.py 
from airflow.models import Variable
import requests
import urllib3
import time
from datetime import datetime
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = 'postgres_default'
SERVICE_NAME = 'psn_vcloud'  # Nome del servizio

# Disabilita i messaggi di avviso per i certificati SSL non verificati (pari a -k in curl)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def renew_and_store_tokens():
    """
    Effettuo la chiamata API di rinnovo
    """
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    # Recupera i dati attuali per il servizio vcloud
    select_query = """
        SELECT psn_org, fqdn, access_token, refresh_token 
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
        raise ValueError(f"Impossibile procedere: Record mancante per il servizio {SERVICE_NAME}.")
        
    psn_org, fqdn, access_token, refresh_token = row

def vcloud_login_refresh(**kwargs):
    """
    Recupera il Refresh Token, esegue la chiamata POST a vCloud per scambiarlo
    con un Access Token temporaneo e lo passa al secondo task tramite XCom.
    """
    print("⚙️ [vCloud Utils] Recupero dell'API Token di console")
    
    # Verifica che il token sia valorizzato
    if not access_token:
        raise Exception("Errore: Il token di accesso non è valorizzato.")
    
    # Costruiamo la URL identica al tuo comando curl di successo
    login_url = f"{fqdn}/oauth/tenant/{psn_org}/token"
    
    # Parametri della URL (Query String)
    query_params = {
        "grant_type": "refresh_token",
        "refresh_token": access_token
    }
    
    headers = {
        "Accept": "application/json"
    }
    
    print(f"📡 Invio richiesta POST di autenticazione per il tenant: {VCLOUD_TENANT}...")
    
    # verify=False equivale a -k (ignora errori SSL)
    response = requests.post(login_url, params=query_params, headers=headers, verify=False, timeout=30)
    
    if response.status_code != 200:
        raise Exception(f"Errore nello scambio del Token (HTTP {response.status_code}): {response.text}")
    
    # Estraiamo il token dal CORPO della risposta JSON (non dagli headers!)
    response_json = response.json()
    token_valido = response_json.get("access_token")
    
    if not token_valido:
        raise Exception("Errore: La risposta di vCloud non contiene il campo 'access_token'.")
        
    print("🔑 [vCloud Utils] Nuovo Access Token ottenuto correttamente e passato alle XCom.")
    return token_valido
