import redis
import pandas as pd
import json
import numpy as np

print("Cargando dataset en memoria...")
columnas = ['latitude', 'longitude', 'confidence', 'area_in_meters']
tipos = {'latitude': 'float32', 'longitude': 'float32', 'confidence': 'float32', 'area_in_meters': 'float32'}

df = pd.read_csv("/data/967_buildings.csv.gz", usecols=columnas, dtype=tipos, compression='gzip')
r = redis.Redis(host='redis', port=6379, decode_responses=True)

zonas_bbox = {
    "Z1": (-33.445, -33.420, -70.640, -70.600),
    "Z2": (-33.420, -33.390, -70.600, -70.550),
    "Z3": (-33.530, -33.490, -70.790, -70.740),
    "Z4": (-33.460, -33.430, -70.670, -70.630),
    "Z5": (-33.470, -33.430, -70.810, -70.760),
}
zonas_area_km2 = {"Z1": 1.5, "Z2": 2.0, "Z3": 3.2, "Z4": 1.2, "Z5": 2.8}

def filtrar_zona(zona_id, conf_min):
    lat_min, lat_max, lon_min, lon_max = zonas_bbox[zona_id]
    return df[
        (df['latitude'] >= lat_min) & (df['latitude'] <= lat_max) &
        (df['longitude'] >= lon_min) & (df['longitude'] <= lon_max) &
        (df['confidence'] >= conf_min)
    ]

print("Response Generator listo y esperando tareas en Redis...")

while True:
    _, dato = r.brpop("cola_tareas")
    tarea = json.loads(dato)
    
    req_id = tarea["id"]
    request = tarea["query"]

    query = request["query"]
    zona = request["zona"]
    conf = request["confidence"]

    data = filtrar_zona(zona, conf)

    if query == "Q1":
        result = len(data)
    elif query == "Q2":
        result = {
            "avg_area": float(data['area_in_meters'].mean()) if not data.empty else 0,
            "total_area": float(data['area_in_meters'].sum()),
            "n": len(data)
        }
    elif query == "Q3":
        result = len(data) / zonas_area_km2[zona]
    elif query == "Q4":
        zona_b = request["zona_b"]
        data_b = filtrar_zona(zona_b, conf)
        densidad_a = len(data) / zonas_area_km2[zona]
        densidad_b = len(data_b) / zonas_area_km2[zona_b]
        result = {"zone_a": densidad_a, "zone_b": densidad_b, "winner": zona if densidad_a > densidad_b else zona_b}
    elif query == "Q5":
        hist, edges = np.histogram(data['confidence'], bins=5, range=(0,1))
        result = [{"bucket": i, "min": float(edges[i]), "max": float(edges[i+1]), "count": int(hist[i])} for i in range(5)]

    r.lpush(f"respuesta_{req_id}", json.dumps(result))
    r.expire(f"respuesta_{req_id}", 10) 
