#  Tarea 1 - Sistemas distribuidos 

Este proyecto implementa un sistema distribuido que incorpora mecanismos de cache para optimizar las consultas de datos  

---

##  Estructura del sistema

1. **Generador de trafico**  
   - Genera consultas aleatorias dependiendo si es Zipf o Uniforme
   - Si la respuesta existe, anota un 'Hit'
   - Si no existe anota un 'Miss' y procesara la consulta en el generador de respuestas en 'cola_consultas'
  
2. **Sistema de cache y colas (Redis/docker-compose.yml)**  
   - Conecta los sistemas
   - Cumple dos funciones: Guarda los resultados calculados sobre reglas estrictas (tamaño de la memoria y politica) y es un sistema de mensajería 

3. **Generador de respuestas**  
   - Cuando le llega el 'Miss' del generador de trafico, calcula los resultados
   - Guarda el resultado en el cache 
   
4. **Almacenamiento de metricas**
   - Se registran metricas del sistema

---

##  Archivos del proyecto

- `docker-compose.yml` → Conecta los contenedores para que funcionen.  
- `traffic-generator` → Simula el comportamiento de usuarios pidiendo información.  
- `response-generator` → Realiza los calculos.
- `metrics` → Registra los hits, miss, latencia, throughput y tasa de evicción.
- `datasets` → Contiene la información sobre la ubicación, tamaño y nivel de confianza de edificaciones, en la Región Metropolitana de Santiago de Chile.
---
##  Compilación

Compilar cada módulo por separado:


1. **Ejecutar el proceso completo:**
   
```bash
docker compose down
docker compose up --build
```

2. **En una terminal secundaria, monitorear las métricas de rendimiento:**

```bash
docker compose logs -f metrics
```

3. **En otra terminal, ver las evicciones en Redis:**

```bash
docker exec -it redis_cache redis-cli info stats | grep evicted_keys
```
---
##  Casos
Para evaluar el comportamiento del sistema bajo distintas condiciones, se deben modificar los siguientes parámetros en los archivos de configuración:

1. **Cambiar el Modo de Tráfico**
   
Esto define si las consultas son en zipf o uniforme
   
- En traffic-generator/main.py

- En la variable MODO_TRAFICO="..."

Puede ser:

"uniforme": equitativa para todas las zonas.

"zipf": sesgada hacia las zonas "populares"

2. **Parámetros de Redis (Tamaño y Política)**

Define el tamaño de memoria y el algoritmo de reemplazo de datos.

- En docker-compose.yml

- En la sección services -> redis -> command

Modificaciones:

--maxmemory [50mb / 200mb / 500mb]: Limita la RAM disponible.

--maxmemory-policy [allkeys-lru / allkeys-lfu]: Cambia el algoritmo de reemplazo. 

3. **Ajuste de TTL (Time To Live)**

Define la persistencia temporal de los datos antes de expirar.

- En response-generator/main.py

- En la línea r.set(key, value, ex=...)

Configuraciones probadas:

ex=5: TTL de corto plazo (evaluación de frescura).

ex=3600: TTL de largo plazo (maximización de Hit Rate).

---

##  Ejemplo de flujo

1. El traffic-generator solicita la densidad de edificios en la Zona 1 con confianza 0.6:
```bash
   TRAFFIC: density:Z1:conf=0.6 -> MISS (Enviado a cola)
```

2. El Response-generator calcula el área sobre el CSV al no encontrar el dato en Redis:

```bash
   [MISS] Calculando densidad para Z1... Resultado guardado en Redis con TTL.
```

3. El Metrics actualiza las estadísticas:

```bash
  Hits=1622 Misses=3528 HitRate=0.31 p50= 0.0011s p95=0.1805s throughput=9,70/s
```

4. Si se repite la consulta antes de que expire el TTL o sea removida por la política (LRU/LFU):

```bash
  TRAFFIC: density:Z1:conf=0.6 -> HIT 
```




