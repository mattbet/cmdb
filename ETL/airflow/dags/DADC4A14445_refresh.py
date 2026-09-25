# DADC4A14445_refresh.py (o vcloud_utils.py)
from airflow.models import Variable
import requests
import urllib3

# Disabilita i messaggi di avviso per i certificati SSL non verificati (pari a -k in curl)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Preleva l'URL e il Tenant in modo sicuro dalle variabili di Airflow
VCLOUD_URL = Variable.get("NORD_url")
VCLOUD_TENANT = Variable.get("ARPA_Lazio_Tenant")

def vcloud_login_refresh(**kwargs):
    """
    Recupera il Refresh Token, esegue la chiamata POST a vCloud per scambiarlo
    con un Access Token temporaneo e lo passa al secondo task tramite XCom.
    """
    print("⚙️ [vCloud Utils] Recupero dell'API Token di console da Airflow Variables...")
    
    # Preleva il token segreto dalle variabili di Airflow
    CONSOLE_TOKEN = Variable.get("DADC4A14445_token")
    
    if not CONSOLE_TOKEN:
        raise Exception("Errore: La variabile 'DADC4A14445_token' non è configurata su Airflow.")
    
    # Costruiamo la URL identica al tuo comando curl di successo
    login_url = f"{VCLOUD_URL}/oauth/tenant/{VCLOUD_TENANT}/token"
    
    # Parametri della URL (Query String)
    query_params = {
        "grant_type": "refresh_token",
        "refresh_token": CONSOLE_TOKEN
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
