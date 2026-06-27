#  Tarea 3 - Sistemas Distribuidos

Esta entrega extiende la Tarea 2 incorporando un pipeline de procesamiento streaming de métricas con **Apache Spark Structured Streaming**, almacenamiento en **Elasticsearch** y visualización en tiempo real mediante **Kibana**.

---

##  Estructura del sistema

1. **Generador de Tráfico**
   - Genera consultas Q1–Q5 en modo Zipf o Uniforme
   - Publica todas las consultas en el tópico Kafka `consultas`
   - Soporta modo spike para simular ráfagas de tráfico (10x)

2. **Kafka Producer y Tópicos**
   - Tópico `consultas`: consultas nuevas
   - Tópico `consultas_retry`: consultas con fallo, en reintento
   - Tópico `consultas_dlq`: consultas que superaron el máximo de reintentos
   - Tópico `metrics-topic`: eventos de métricas por consulta procesada ← **nuevo**

3. **Consumers Kafka** (escalables horizontalmente)
   - Consumen desde `consultas` y `consultas_retry`
   - Verifican caché Redis: HIT → respuesta inmediata, MISS → procesamiento
   - En caso de falla publican en `consultas_retry` hasta 3 reintentos
   - Al superar reintentos, publican en `consultas_dlq`
   - Publican cada evento procesado en `metrics-topic`

4. **Sistema Caché (Redis)**
   - TTL: 5 minutos
   - Política de remoción: `allkeys-lfu`
   - Tamaño máximo: 2 MB
   - Actúa como canal de control para simular fallos y spikes

5. **Sistema de Métricas (mejorado)**
   - Consume eventos desde `metrics-topic`
   - Calcula y publica en Elasticsearch: throughput, latencia p50/p95, hit rate, retry rate, DLQ rate, backlog
   - Imprime resumen en consola cada 5 segundos

6. **Apache Spark Structured Streaming** ← **nuevo**
   - Job PySpark que lee continuamente desde `metrics-topic`
   - Aplica ventanas deslizantes (1 min de ventana, slide de 30 s, watermark de 2 min)
   - Calcula por ventana: throughput, latencia p50/p95, hit rate, retry rate, recovery rate, DLQ count
   - Escribe resultados agregados en el índice `metrics-aggregated` de Elasticsearch

7. **Elasticsearch** ← **nuevo**
   - Almacena documentos del sistema de métricas en `system-metrics`
   - Almacena agregaciones de Spark en `metrics-aggregated`
   - Single-node, sin seguridad (modo demo)

8. **Kibana** ← **nuevo**
   - Dashboard interactivo con visualizaciones en tiempo real
   - Se conecta a Elasticsearch en `http://elasticsearch:9200`

---

##  Estructura de archivos

```text
sistemas-distribuidos/
├── docker-compose.yml              → Orquesta todos los contenedores
├── data/
│   └── [DATASET CSV]               → Dataset Google Open Buildings (descargar aparte)
├── traffic-generator/
│   └── main.py                     → Genera consultas y publica en Kafka
├── response-generator/
│   ├── consumer.py                 → Consumer Kafka con reintentos, DLQ y métricas
│   └── Dockerfile
├── metrics/
│   ├── main.py                     → Monitor de métricas → Elasticsearch
│   └── Dockerfile
├── spark/
│   ├── spark_streaming.py          → Job PySpark con ventanas deslizantes → ES
│   └── Dockerfile
└── elasticsearch/
    └── init.sh                     → Crea index templates al iniciar (opcional)
```

---

##  Configuración previa

Antes de ejecutar, descarga el dataset y ubícalo en la carpeta `data/`:

```text
sistemas-distribuidos/
├── docker-compose.yml
├── data/
│   └── [AQUÍ VA EL ARCHIVO DE DATASET]
├── response-generator/
├── traffic-generator/
├── metrics/
└── spark/
```

---

##  Compilación y ejecución

```bash
# Levantar todo el sistema
docker compose down
docker compose up --build

# Ver métricas en tiempo real
docker compose logs -f metrics

# Ver logs de los consumers
docker compose logs -f consumer

# Ver logs del job Spark
docker compose logs -f spark

# Acceder a Kibana
# http://localhost:5601

# Verificar índices en Elasticsearch
curl http://localhost:9200/_cat/indices?v
```

---

##  Configuración de Kibana

### 1. Crear Data View

Ir a: `http://localhost:5601/app/management/kibana/dataViews`

| Name | Index pattern | Timestamp field |
|------|--------------|-----------------|
| System Metrics | `system-metrics*` | `@timestamp` |
| Metrics Aggregated (Spark) | `metrics-aggregated*` | `window_start` |

### 2. Dashboard recomendado

Ir a `Analytics → Dashboards → Create dashboard` y agregar:

| Panel | Tipo | Campo |
|-------|------|-------|
| Throughput en el tiempo | Line | `throughput_window_qps` |
| Hit Rate | Line | `hit_rate` |
| Latencia p50 vs p95 | Line (2 series) | `latency_p50_ms`, `latency_p95_ms` |
| Retry Rate | Bar | `retry_rate` |
| DLQ Total | Metric | `dlq_total` |
| Backlog Redis | Line | `backlog_redis` |

---

##  Escenarios de Evaluación

### Escenario 1 — Operación Normal (3 consumers)

```bash
docker compose up --build -d
docker compose logs -f metrics
# Observar dashboard en Kibana: http://localhost:5601
```

### Escenario 2 — 1 Consumer vs 3 Consumers

```bash
# Resetear estado
docker exec redis_cache redis-cli flushall
curl -X DELETE http://localhost:9200/system-metrics

# Escalar a 1 consumer
docker compose up -d --scale consumer=1
# Esperar 3-5 minutos → capturar screenshot del dashboard

# Resetear estado
docker exec redis_cache redis-cli flushall
curl -X DELETE http://localhost:9200/system-metrics

# Escalar a 3 consumers
docker compose up -d --scale consumer=3
# Verificar balanceo en Kafka
docker compose logs kafka | grep "Stabilized group"
# Esperar 3-5 minutos → capturar screenshot y comparar
```


### Escenario 3 — Falla Temporal del Generador

```bash
# Simular caída
docker exec redis_cache redis-cli set generador_activo 0

# Observar en Kibana: retry_rate sube, throughput baja
# Esperar ~30 segundos

# Restaurar
docker exec redis_cache redis-cli set generador_activo 1

# Observar en Kibana: sistema se recupera, recovery_time aparece en logs
docker compose logs -f metrics
```

### Escenario 4 — Reintentos y Dead Letter Queue

```bash
# Simular caída prolongada para forzar DLQ
docker exec redis_cache redis-cli set generador_activo 0

# Observar reintentos y DLQ en tiempo real
docker compose logs -f consumer
# Error procesando ...: Fallo temporal simulado
# RETRY query_id=... intento=1/3
# RETRY query_id=... intento=2/3
# DLQ  query_id=... reintentos=3

# Observar en Kibana: dlq_total aumenta, retry_rate > 0
# Restaurar
docker exec redis_cache redis-cli set generador_activo 1
```

### Escenario 5 — Spike de Tráfico

```bash
# Activar spike (10x más consultas)
docker exec redis_cache redis-cli set traffic_mode spike

# Observar en Kibana: throughput se dispara, backlog_redis crece
docker compose logs -f metrics

# Volver a modo normal
docker exec redis_cache redis-cli set traffic_mode normal

# Observar cómo el backlog se drena gradualmente
```

---

##  Flujo del sistema

```
Generador de Tráfico
        │
        ▼
  [Kafka: consultas]
        │
        ▼
  Consumers (x1 o x3)
   ├── Redis HIT → respuesta inmediata
   └── Redis MISS → procesar → guardar en Redis
        │
        ├──→ [Kafka: consultas_retry] (falla)
        ├──→ [Kafka: consultas_dlq]  (máx reintentos)
        └──→ [Kafka: metrics-topic]  ← evento por cada consulta
                    │
          ┌─────────┴──────────┐
          ▼                    ▼
    Sistema Métricas     Spark Streaming
    (main.py)            (ventanas 1min/30s)
          │                    │
          ▼                    ▼
    Elasticsearch         Elasticsearch
    system-metrics    metrics-aggregated
                  │
                  ▼
               Kibana
            (Dashboard)
```
