#  Tarea 2 - Sistemas distribuidos 

Este proyecto se evoluciona la Tarea 1 incorporando Apache Kafka como sistema de mensajería, agregando tolerancia a fallos mediante reintentos automáticos, Dead Letter Queue (DLQ) y escalamiento horizontal con múltiples consumidores. 

---

##  Estructura del sistema

1. **Generador de trafico**  
  - Genera consultas aleatorias en modo Zipf o Uniforme
  - Consulta Redis primero: si hay HIT responde de inmediato
  - Si hay MISS, publica la consulta en el tópico Kafka consultas
  - Soporta modo spike para simular ráfagas de tráfico
    
2. **Kafka Producer**
  - Recibe las consultas del generador de tráfico
  - Administra tres tópicos: consultas, consultas_retry y consultas_dlq
  - Desacopla el generador de tráfico del procesamiento

3. **Consumers Kafka**
  - Consumen mensajes desde consultas y consultas_retry
  - Consultan Redis: si hay HIT retornan inmediatamente
  - Si hay MISS, procesan la consulta y guardan el resultado en Redis
  - En caso de falla, reenvían la consulta a consultas_retry
  - Escalables horizontalmente (1, 2, 3 o más réplicas)

4. **Dead Letter Queue (DLQ)**
  - Las consultas que superan el máximo de reintentos se envían al tópico consultas_dlq
  - Permite auditoría de consultas no resueltas sin pérdida de información

5. **Generador de respuestas**  
   - Cuando le llega el 'Miss' del generador de trafico, calcula los resultados
   - Guarda el resultado en el cache 
   
6. **Sistema Cache**
   - Almacena resultados calculados con TTL de 5 minutos
   - Política de remoción: allkeys-lfu
   - Tamaño máximo: 2MB
   - Actúa como canal de control para simular fallos y spikes

7. **Sistema de Métricas**
  - Registra throughput, latencia (p50, p95), HitRate, reintentos, DLQ, drops y backlog
  - Imprime resumen cada 10 consultas procesadas
  - Muestra eventos de RETRY y DLQ en tiempo real

---

##  Archivos del proyecto

- `docker-compose.yml` → Conecta los contenedores para que funcionen.  
- `traffic-generator` → Simula el comportamiento de usuarios pidiendo información.  
- `response-generator` → Realiza los calculos.
- `metrics` → Registra los hits, miss, latencia, throughput y tasa de evicción.
- `datasets` → Contiene la información sobre la ubicación, tamaño y nivel de confianza de edificaciones, en la Región Metropolitana de Santiago de Chile.
---
## Configuración previa 

Antes de ejecutar la tarea, es necesario descargar y ubicar el dataset de pruebas, ya que por su tamaño no se incluye en el repositorio.

1.- Crea una carpeta llamada data` en la raíz del proyecto.

2.- Descarga el archivo del dataset y colócalo dentro de esa carpeta.

La estructura de tus archivo deberia verse asi antes de continuar:

```text
sistemas-distribuidos/
├── docker-compose.yml
├── data/
│   └── [AQUÍ VA EL ARCHIVO DE DATASET]
├── response-generator
├── traffic-generator
└── metrics
```

---

##  Compilación

# Levantar todo el sistema
docker compose down
docker compose up --build

# Ver métricas en tiempo real
docker compose logs -f metrics

# Ver logs de los consumers
docker compose logs -f consumer

# Ver logs del generador de tráfico
docker compose logs -f traffic-generator

---

##  Escenarios Evalución
**Escenario 1 — Sistema Base (Tarea 1, sin Kafka)**
bashgit checkout tarea1
docker compose up --build
docker compose logs -f traffic-generator

**Escenario 2 — Kafka + 1 Consumer**
En docker-compose.yml, asegurarse que consumer tenga replicas: 1, luego:
bashdocker compose up --build
docker compose logs -f metrics

**Escenario 3 — Kafka + Múltiples Consumers**
bash# Escalar a 3 consumers
docker compose up --scale consumer=3 -d

# Verificar los 3 consumers activos
docker compose ps | grep consumer

# Verificar balanceo en Kafka (debe decir "with 3 members")
docker compose logs kafka | grep "Stabilized group"

**Escenario 4 — Falla Temporal del Generador de Respuestas**
bash# Simular caída
docker exec redis_cache redis-cli set generador_activo 0

# Observar reintentos y DLQ en los logs
docker compose logs -f consumer

# Restaurar el generador
docker exec redis_cache redis-cli set generador_activo 1

**Escenario 5 — Reintentos Automáticos**
El consumer tiene un 20% de probabilidad de fallo aleatorio activo por defecto. No requiere configuración adicional. Observar en los logs:
bashdocker compose logs -f consumer
# Error procesando ...: Fallo temporal simulado
# Reintento 1/3 → ...
# MISS CACHE → ... ← resuelto exitosamente en reintento

**Escenario 6 — Spike de Tráfico**
bash# Activar spike (10x más consultas)
docker exec redis_cache redis-cli set traffic_mode spike

# Observar backlog y throughput en métricas
docker compose logs -f metrics

# Volver a modo normal
docker exec redis_cache redis-cli set traffic_mode normal

**Escenario 7 — Recuperación ante Fallos**
bash# 1. Limpiar estado
docker exec redis_cache redis-cli flushall

# 2. Simular caída
docker exec redis_cache redis-cli set generador_activo 0

# 3. Esperar ~30 segundos (las consultas van a retry/DLQ pero NO se pierden)
docker compose logs -f metrics

# 4. Recuperar
docker exec redis_cache redis-cli set generador_activo 1

# 5. Observar que el sistema retoma el procesamiento normal
docker compose logs -f consumer
