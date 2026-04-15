import redis
import pandas as pd
import json
import numpy as np

print("Cargando dataset...")

df = pd.read_csv("small_buildings.csv")

print("Dataset cargado")

r = redis.Redis(
    host='redis',
    port=6379,
    decode_responses=True
)

zonas_bbox = {

    "Z1": (-33.445, -33.420, -70.640, -70.600),
    "Z2": (-33.420, -33.390, -70.600, -70.550),
    "Z3": (-33.530, -33.490, -70.790, -70.740),
    "Z4": (-33.460, -33.430, -70.670, -70.630),
    "Z5": (-33.470, -33.430, -70.810, -70.760),
}

zonas_area_km2 = {

    "Z1": 1.5,
    "Z2": 2.0,
    "Z3": 3.2,
    "Z4": 1.2,
    "Z5": 2.8
}


def filtrar_zona(zona, conf):

    lat_min, lat_max, lon_min, lon_max = zonas_bbox[zona]

    data = df[
        (df['latitude'] >= lat_min) &
        (df['latitude'] <= lat_max) &
        (df['longitude'] >= lon_min) &
        (df['longitude'] <= lon_max) &
        (df['confidence'] >= conf)
    ]

    return data
def Q1(data):

    return len(data)


def Q2(data):

    return {

        "avg_area": float(data['area_in_meters'].mean()),
        "total_area": float(data['area_in_meters'].sum()),
        "n": len(data)
    }
  
def Q3(zona, data):

    count = len(data)

    return count / zonas_area_km2[zona]

def Q4(zonaA, zonaB, conf):

    dataA = filtrar_zona(zonaA, conf)
  
  
    dataB = filtrar_zona(zonaB, conf)

    dA = Q3(zonaA, dataA)

    dB = Q3(zonaB, dataB)

    return {

        "zoneA": dA,
        "zoneB": dB,
        "winner": zonaA if dA > dB else zonaB
    }

def Q5(data):

    hist, edges = np.histogram(
        data['confidence'],
        bins=5,
        range=(0,1)
    )

    result = []

    for i in range(5):
      
        result.append({

            "bucket": i,
            "min": float(edges[i]),
            "max": float(edges[i+1]),
            "count": int(hist[i])
        })

    return result


print("Esperando consultas...")

while True:

    dato = r.brpop("cola_consultas")

    request = json.loads(dato[1])

    zona = request["zona"]
    query = request["query"]
    conf = request["confidence"]

    print(f"DEBUG: Procesando {query} para {zona} (Conf: {conf})")

    key = f"{query}:{zona}:conf={conf}"

    data = filtrar_zona(zona, conf)

    if query == "Q1":

        result = Q1(data)

    elif query == "Q2":

        result = Q2(data)

    elif query == "Q3":

        result = Q3(zona, data)

    elif query == "Q4":

        zonaB = np.random.choice(
            ["Z1","Z2","Z3","Z4","Z5"]
        )

        result = Q4(zona, zonaB, conf)

    elif query == "Q5":

        result = Q5(data)
      
    else:

        result = "Query no válida"

    r.set(
        key,
        json.dumps(result),
        ex=60
    )
    print(f"DEBUG: Resultado guardado en caché para {key}")


      
