import os
import json
import uuid
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import requests
import boto3

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

# Si no se define DDB_TABLE en serverless.yml, usa la tabla creada en resources (TablaWebScrapping2)
DDB_TABLE = os.environ.get("DDB_TABLE", "TablaWebScrapping2")
ARCGIS_LAYER_URL = os.environ.get(
    "ARCGIS_LAYER_URL",
    "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/SismosReportados/MapServer/0/query"
)
PREF_REPLACE_TABLE = os.environ.get("PREF_REPLACE_TABLE", "false").lower() == "true"

dynamodb = boto3.resource("dynamodb")


def ensure_table():
    """Devuelve el objeto Table; lanza ValueError legible si falta DDB_TABLE."""
    if not DDB_TABLE:
        raise ValueError(
            "Variable de entorno DDB_TABLE no definida. Configure DDB_TABLE en serverless.yml."
        )
    LOG.debug("Usando tabla DynamoDB: %s", DDB_TABLE)
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
        "f": "geojson",
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
        item_id = str(
            attrs.get("code") or attrs.get("objectid") or attrs.get("OBJECTID") or uuid.uuid4()
        )

        item = {
            "id": item_id,
            "referencia": str(attrs.get("ref") or attrs.get("referencia") or ""),
            "fechaevento": fecha_iso or "",
            "fecha": str(attrs.get("fecha") or ""),
            "hora": str(attrs.get("hora") or ""),
            # mantengo como string para evitar conversiones innecesarias,
            # los valores numéricos en 'raw' serán convertidos por convert_numbers
            "magnitud": str(attrs.get("magnitud") or attrs.get("mag") or ""),
            "lat": str(lat) if lat != "" else "",
            "lon": str(lon) if lon != "" else "",
            "profundidad": str(attrs.get("profundidad") or attrs.get("prof") or ""),
            "raw": {k: (v if v is not None else "") for k, v in attrs.items()},
        }
        items.append(item)
    return items


def scan_all_ids(table):
    ids = []
    kwargs = {"ProjectionExpression": "id"}
    while True:
        resp = table.scan(**kwargs)
        ids.extend([it["id"] for it in resp.get("Items", []) if "id" in it])
        last = resp.get("LastEvaluatedKey")
        if not last:
            break
        kwargs["ExclusiveStartKey"] = last
    return ids


def clear_table_by_ids(table, ids):
    if not ids:
        return
    with table.batch_writer() as batch:
        for _id in ids:
            batch.delete_item(Key={"id": _id})


def convert_numbers(obj):
    """
    Convierte floats/ints (y números en estructuras anidadas) a Decimal para DynamoDB.
    Mantiene strings y otros tipos tal cual.
    """
    if isinstance(obj, dict):
        return {k: convert_numbers(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_numbers(v) for v in obj]
    if isinstance(obj, float) or isinstance(obj, int):
        # Usar str() evita problemas de precisión y preserva formato
        return Decimal(str(obj))
    return obj


def upsert_items(table, items):
    if not items:
        return
    with table.batch_writer() as batch:
        for it in items:
            # convert_numbers maneja floats/ints anidados (por ejemplo en 'raw')
            batch.put_item(Item=convert_numbers(it))


def lambda_handler(event, context):
    LOG.info("Inicio Lambda: fetch latest sismos")

    # Obtener la tabla (verifica DDB_TABLE)
    try:
        table = ensure_table()
    except ValueError as e:
        LOG.error("Configuración inválida: %s", e)
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "CONFIG_ERROR", "message": str(e)}),
        }

    # Determinar límite (queryStringParameters ?limit=)
    limit = 10
    if isinstance(event, dict):
        qs = event.get("queryStringParameters") or {}
        if qs and qs.get("limit"):
            try:
                limit = int(qs.get("limit"))
            except Exception:
                LOG.warning("Parámetro limit no válido, usando 10")

    # Fetch
    try:
        items = fetch_latest_sismos(limit=limit)
        LOG.info("Sismos obtenidos: %d", len(items))
    except requests.HTTPError as e:
        LOG.exception("HTTP error fetching ArcGIS layer")
        return {
            "statusCode": 502,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "FETCH_ERROR", "detail": str(e)}),
        }
    except Exception as e:
        LOG.exception("Error en fetch_latest_sismos")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "FETCH_ERROR", "detail": str(e)}),
        }

    # Escribir en DynamoDB
    try:
        if PREF_REPLACE_TABLE:
            LOG.info("PREF_REPLACE_TABLE=true -> borrando tabla (solo IDs) antes de insertar")
            ids = scan_all_ids(table)
            if ids:
                clear_table_by_ids(table, ids)

        upsert_items(table, items)
    except Exception as e:
        LOG.exception("Error escribiendo en DynamoDB")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "DDB_ERROR", "detail": str(e)}),
        }

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"count": len(items), "items": items}),
    }
