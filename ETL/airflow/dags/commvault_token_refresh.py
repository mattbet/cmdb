import time
import requests
from datetime import datetime, timezone

# ============================================================
# CONFIGURAZIONE BaaS
# ============================================================
BAAS_FQDN = "baas-nord.console.polostrategiconazionale.it"
BAAS_BASE_URL = f"https://{BAAS_FQDN}"
API_BASE_PATH = "/commandcenter/api"
API_REFRESH_TOKEN = f"{API_BASE_PATH}/v4/AccessToken/Renew"
# Il token viene rinnovato preventivamente se mancano
# meno di 5 minuti alla scadenza.
TOKEN_REFRESH_MARGIN = 300
REQUEST_TIMEOUT = 30

def build_url(api_path: str) -> str:
    """Costruisce la URL completa dell'API."""
    return f"{BAAS_BASE_URL}{api_path}"

# ============================================================
# CONTROLLO SCADENZA TOKEN
# ============================================================
def token_is_expired_or_expiring(
    token_expiry_timestamp: int,
    margin_seconds: int = TOKEN_REFRESH_MARGIN
) -> bool:
    current_timestamp = int(time.time())
    return current_timestamp >= (
        token_expiry_timestamp - margin_seconds
    )

# ============================================================
# REFRESH TOKEN
# ============================================================
def refresh_access_token(
    access_token: str,
    refresh_token: str
) -> dict:
    url = build_url(API_REFRESH_TOKEN)
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authtoken": access_token,
    }
    payload = {
        "accessToken": access_token,
        "refreshToken": refresh_token,
    }
    response = requests.post(
        url,
        headers=headers,
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    data = response.json()
    return {
        "access_token":
            data["accessToken"],
        "refresh_token":
            data["refreshToken"],
        "expiry_timestamp":
            data["tokenExpiryTimestamp"],
        "refresh_expiry_timestamp":
            data.get("refreshTokenExpiryTimestamp"),
        "renewable_until_timestamp":
            data.get("renewableUntilTimestamp"),
    }

# ============================================================
# CONTROLLO PREVENTIVO TOKEN
# ============================================================
def ensure_valid_token(token_info: dict) -> dict:
    expiry_timestamp = token_info["expiry_timestamp"]
    if token_is_expired_or_expiring(expiry_timestamp):
        print(
            "Access token scaduto o prossimo alla scadenza. "
            "Eseguo refresh preventivo."
        )
        token_info = refresh_access_token(
            token_info["access_token"],
            token_info["refresh_token"]
        )
        print("Access token rinnovato.")
    return token_info

# ============================================================
# CHIAMATA API CON GESTIONE AUTOMATICA HTTP 401
# ============================================================
def baas_request(
    method: str,
    api_path: str,
    token_info: dict,
    json=None,
    params=None
):
    """
    Esegue una chiamata alle API BaaS.
    Flusso:
      1. controlla preventivamente la scadenza
      2. esegue la richiesta
      3. se riceve HTTP 401:
           - refresh token
           - retry della richiesta
      4. il retry viene eseguito UNA SOLA VOLTA
    Restituisce:
        response, token_info
    """
    # --------------------------------------------------------
    # 1. Controllo preventivo scadenza
    # --------------------------------------------------------
    token_info = ensure_valid_token(token_info)
    url = build_url(api_path)
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authtoken": token_info["access_token"],
    }
    # --------------------------------------------------------
    # 2. Prima chiamata API
    # --------------------------------------------------------
    response = requests.request(
        method=method,
        url=url,
        headers=headers,
        json=json,
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    # --------------------------------------------------------
    # 3. Nessun problema di autenticazione
    # --------------------------------------------------------
    if response.status_code != 401:
        response.raise_for_status()
        return response, token_info
    # --------------------------------------------------------
    # 4. HTTP 401
    # --------------------------------------------------------
    print(
        "Ricevuto HTTP 401. "
        "Eseguo refresh del token e retry."
    )
    token_info = refresh_access_token(
        token_info["access_token"],
        token_info["refresh_token"]
    )
    # --------------------------------------------------------
    # IMPORTANTE:
    # ricreiamo gli header con il NUOVO access token
    # --------------------------------------------------------
    headers["Authtoken"] = token_info["access_token"]
    # --------------------------------------------------------
    # 5. Retry
    # --------------------------------------------------------
    response = requests.request(
        method=method,
        url=url,
        headers=headers,
        json=json,
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    # --------------------------------------------------------
    # 6. Se fallisce ancora NON facciamo altri refresh
    # --------------------------------------------------------
    response.raise_for_status()
    return response, token_info