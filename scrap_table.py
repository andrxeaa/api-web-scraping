import os
import uuid
import requests
import boto3
from datetime import datetime

DDB_TABLE = os.environ.get("DDB_TABLE")
# Endpoint ArcGIS (IGP) - capa "Sismos Reportados"
ARCGIS_LAYER_URL = "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/SismosReportados/MapServer/0/query"

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DDB_TABLE)

def fetch_latest_sismos(limit=10):
    params = {
        "where": "1=1",
        "outFields": "objectid,fechaevento,fecha,hora,mag,magnitud,ref,lat,lon,profundidad,prof,intento,departamento,code",
        "orderByFields": "fechaevento DESC",
        "resultRecordCount": limit,
        "f": "geojson"
    }
    resp = requests.get(ARCGIS_LAYER_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    features = data.get("features", [])
    items = []
    for feat in features:
        attrs = feat.get("properties") or feat.get("attributes") or {}
        # Normalizar campos y convertir fechas (si vienen en ms)
        fechaevento = attrs.get("fechaevento")
        # ArcGIS date sometimes comes as milliseconds since epoch; intentar parsear
        if isinstance(fechaevento, (int, float)):
            try:
                fecha_iso = datetime.utcfromtimestamp(fechaevento/1000).isoformat()
            except Exception:
                fecha_iso = str(fechaevento)
        else:
            fecha_iso = str(fechaevento) if fechaevento is not None else None

        item = {
            "id": str(attrs.get("code") or attrs.get("objectid") or str(uuid.uuid4())),
            "fechaevento": fecha_iso,
            "fecha": str(attrs.get("fecha") or ""),
            "hora": str(attrs.get("hora") or ""),
            "magnitud": str(attrs.get("magnitud") or attrs.get("mag") or ""),
            "referencia": str(attrs.get("ref") or ""),
            "lat": str(attrs.get("lat") or ""),
            "lon": str(attrs.get("lon") or ""),
            "profundidad": str(attrs.get("profundidad") or attrs.get("prof") or ""),
            "departamento": str(attrs.get("departamento") or ""),
            # raw attributes for future use:
            "raw": {k: (v if v is not None else "") for k, v in attrs.items()}
        }
        items.append(item)
    return items

def clear_table():
    # Solo para mantener la tabla con últimos N: escanea y borra (coste de RCU en tablas grandes)
    resp = table.scan(ProjectionExpression="id")
    with table.batch_writer() as batch:
        for it in resp.get("Items", []):
            batch.delete_item(Key={"id": it["id"]})

def lambda_handler(event, context):
    try:
        items = fetch_latest_sismos(limit=10)
    except Exception as e:
        return {"statusCode": 502, "body": f"Error fetching data: {str(e)}"}

    # Opción A: reemplazar todo (borra los existentes y escribe los nuevos)
    try:
        # borrar (opcional)
        scan = table.scan(ProjectionExpression="id")
        if scan.get("Items"):
            with table.batch_writer() as batch:
                for each in scan["Items"]:
                    batch.delete_item(Key={"id": each["id"]})
    except Exception:
        # no fatal; continuar con inserción
        pass

    # Insertar/actualizar
    with table.batch_writer() as batch:
        for it in items:
            batch.put_item(Item=it)

    return {
        "statusCode": 200,
        "body": {"count": len(items), "data": items}
    }
