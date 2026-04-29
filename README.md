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
---
##  Compilación

Compilar cada módulo por separado:


1. Ejecutar el proceso completo:
   
```bash
docker compose down
docker compose up --build
```

2. En una terminal secundaria, monitorear las métricas de rendimiento::

```bash
docker compose logs -f metrics
./usuarios
```

3. En otra terminal, ver las evicciones en Redis

```bash
docker exec -it redis_cache redis-cli info stats | grep evicted_keys
./reportar
```
---

