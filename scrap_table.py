import os
import json
import uuid
import logging
from datetime import datetime, timezone, timedelta

import requests
import boto3

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

# Config desde env (no creamos table todavía)
DDB_TABLE = os.environ.get("DDB_TABLE")
ARCGIS_LAYER_URL = os.environ.get(
    "ARCGIS_LAYER_URL",
    "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/SismosReportados/MapServer/0/query"
)
PREF_REPLACE_TABLE = os.environ.get("PREF_REPLACE_TABLE", "false").lower() == "true"

# Crear cliente/recursos boto3 aquí (pero no table hasta verificar DDB_TABLE)
dynamodb = boto3.resource("dynamodb")

def ensure_table():
    """Devuelve el objeto Table; lanza ValueError legible si falta DDB_TABLE."""
    if not DDB_TABLE:
        raise ValueError("Variable de entorno DDB_TABLE no definida. Configure DDB_TABLE en serverless.yml.")
    return dynamodb.Table(DDB_TABLE)

def parse_date(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        try:
            utc_dt = datetime.utcfromtimestamp(val / 1000).replace(tzinfo=timezone.utc)
            pet = utc_dt.astimezone(timezone(timedelta(hours=-5)))
            return pet.isoformat()
        except Exception:
            return str(val)
    return str(val)

def fetch_latest_sismos(limit=10):
    params = {
        "where": "1=1",
        "outFields": "*",
        "orderByFields": "fechaevento DESC",
        "resultRecordCount": limit,
        "f": "geojson"
    }
    resp = requests.get(ARCGIS_LAYER_URL, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    features = data.get("features", []) or []
    items = []
    for feat in features:
        attrs = feat.get("properties") or feat.get("attributes") or {}
        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates") or []
        lon = coords[0] if len(coords) >= 1 else (attrs.get("lon") or "")
        lat = coords[1] if len(coords) >= 2 else (attrs.get("lat") or "")

        fecha_iso = parse_date(attrs.get("fechaevento") or attrs.get("FECHAEVENTO"))
        item_id = str(attrs.get("code") or attrs.get("objectid") or attrs.get("OBJECTID") or uuid.uuid4())

        item = {
            "id": item_id,
            "referencia": str(attrs.get("ref") or attrs.get("referencia") or ""),
            "fechaevento": fecha_iso or "",
            "fecha": str(attrs.get("fecha") or ""),
            "hora": str(attrs.get("hora") or ""),
            "magnitud": str(attrs.get("magnitud") or attrs.get("mag") or ""),
            "lat": str(lat) if lat != "" else "",
            "lon": str(lon) if lon != "" else "",
            "profundidad": str(attrs.get("profundidad") or attrs.get("prof") or ""),
            "raw": {k: (v if v is not None else "") for k, v in attrs.items()}
        }
        items.append(item)
    return items

def scan_all_ids(table):
    ids = []
    kwargs = {"ProjectionExpression": "id"}
    while True:
        resp = table.scan(**kwargs)
        ids.exten
