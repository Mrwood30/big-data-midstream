# Big Data en el midstream y las operaciones de bombeo

**Estudiante:** Yoander Frank Madera Florez
**Materia:** Electiva II: Grandes Volúmenes de Datos
**Institución:** Fundación Universitaria San Mateo
**Año:** 2026

---

## ¿De qué trata este repositorio?

Aquí guardo los trabajos que hice en la materia sobre un mismo tema: cómo se usa el Big Data en el **midstream**, que es la parte de la industria del petróleo y el gas que transporta los hidrocarburos por ductos, y en especial en las **operaciones de bombeo**.

| Carpeta | Trabajo | Formato |
|---|---|---|
| [`tarea1-analisis-sector/`](tarea1-analisis-sector/) | Análisis de aplicaciones de Big Data en el sector transporte (ductos) | Documento LaTeX en formato IEEE |
| [`tarea2-nube-gcp/`](tarea2-nube-gcp/) | Almacenamiento y procesamiento en la nube con datos públicos de ductos | Notebook de Jupyter (Google Colab) |

---

## Tarea 1: Análisis de aplicaciones de Big Data en el sector

Documento de dos páginas en formato IEEE de dos columnas. Explica cómo se aplican las 5 V del Big Data (volumen, velocidad, variedad, veracidad y valor) en el transporte por ductos, qué tecnologías se usan (Hadoop, Spark, NoSQL, almacenes en la nube y herramientas de visualización) y qué beneficios se han reportado en mantenimiento predictivo, ahorro de energía en el bombeo y detección de fugas.

**Cómo verlo:** subir el archivo `.tex` a [Overleaf](https://www.overleaf.com) y compilarlo con pdfLaTeX.

---

## Tarea 2: Almacenamiento y procesamiento en la nube (GCP)

En este trabajo armé un flujo completo en **Google Cloud Platform** con datos abiertos de caudal y capacidad de 18 ductos de petróleo y gas de Canadá, publicados por el **Canada Energy Regulator (CER)**.

### Arquitectura

Usé una arquitectura por capas (bronce, plata y oro):

```
Datos abiertos del CER (CSV)
        │  Ingesta con Python (Colab)
        ▼
Cloud Storage /raw/      → capa BRONCE: archivos originales
        │  Limpieza con Spark (Dataproc)
        ▼
Cloud Storage /silver/   → capa PLATA: datos limpios en Parquet
        │  Indicadores con Spark
        ▼
Cloud Storage /gold/     → capa ORO: utilización de los ductos
        │  Carga
        ▼
BigQuery                 → consultas SQL y gráficos
```

### Tecnologías

- **Google Cloud Storage:** el *data lake* donde se guardan las tres capas.
- **Dataproc (Hadoop + Spark):** un clúster de un solo nodo que procesa los datos y se borra al terminar.
- **BigQuery:** consultas SQL sobre los resultados.
- **Google Colab:** el notebook que coordina todo el proceso.

### Archivos

| Archivo | Descripción |
|---|---|
| `midstream_gcp_bigdata.ipynb` | Notebook ejecutado, con todas las explicaciones, resultados y gráficas |
| `procesar_midstream.py` | Programa de Spark que corre en el clúster de Dataproc |
| `evidencias/` | Capturas de la consola de Google Cloud que demuestran la ejecución |

### Resultados de la ejecución

- Se descargaron **49,3 MB** de datos de 18 ductos (10 de petróleo y líquidos y 8 de gas natural).
- El trabajo de Spark en Dataproc terminó correctamente en **2 min 48 s**.
- En BigQuery quedaron **4.570 filas** de utilización mensual y **130 filas** del resumen por ducto y año.

### Indicador principal

La **utilización** de cada ducto se calcula como **caudal / capacidad**. Si está cerca del 100 %, el ducto va casi lleno y sus estaciones de bombeo trabajan al límite.

### Cómo ejecutarlo

1. Tener un proyecto de Google Cloud con las APIs de Dataproc, Compute Engine, BigQuery y Cloud Storage activadas.
2. Abrir el notebook en [Google Colab](https://colab.research.google.com).
3. Cambiar `PROJECT_ID` y `BUCKET` en la celda de configuración.
4. Ejecutar las celdas en orden. La última borra el clúster para no gastar crédito.

> En este repositorio **no hay claves ni credenciales**: la autenticación se hace en Colab con la cuenta de Google de cada persona.

---

## Fuente de datos

Canada Energy Regulator, *Pipeline Throughput and Capacity Data*. Datos publicados bajo la Licencia de Gobierno Abierto de Canadá:
https://open.canada.ca/data/en/dataset/dc343c43-a592-4a27-8ee7-c77df56afb34
