import requests
import pandas as pd

#API = "http://127.0.0.1:8000/api"
API = "http://backend:8000/api"

# ==============================
# BASE REQUEST
# ==============================

def _request(endpoint, params=None):
    r = requests.get(f"{API}{endpoint}", params=params)
    r.raise_for_status()
    return pd.DataFrame(r.json()["data"])


# ==============================
# ENDPOINTS
# ==============================

def get_cnes():
    return _request("/cnes")


def get_procedimentos():
    return _request("/procedimentos")


def get_calendario():
    return _request("/calendario")


def get_analitico(params=None):
    return _request("/analitico", params=params)