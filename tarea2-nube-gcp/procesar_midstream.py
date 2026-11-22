# procesar_midstream.py
# Script de Spark que corre dentro del clúster de Dataproc.
# Lee los CSV crudos (capa bronce), los limpia (capa plata) y calcula indicadores (capa oro).

import re
import sys

from pyspark.sql import SparkSession, functions as F

# La ruta base llega como argumento, por ejemplo gs://midstream-bigdata-datos
BASE = sys.argv[1].rstrip("/")

# Ductos que vamos a procesar y su tipo (petróleo y líquidos o gas natural)
DUCTOS = {
    "keystone": "petroleo", "trans-mountain": "petroleo", "trans-northern": "petroleo",
    "cochin": "petroleo", "enbridge-mainline": "petroleo", "norman-wells": "petroleo",
    "southern-lights": "petroleo", "westspur": "petroleo", "enbridge-line9": "petroleo",
    "montreal": "petroleo",
    "ngtl": "gas", "tcpl-mainline": "gas", "foothills": "gas", "alliance": "gas",
    "westcoast": "gas", "tqm": "gas", "mnp": "gas", "brunswick": "gas",
}

spark = SparkSession.builder.appName("midstream-cer").getOrCreate()


def limpiar_nombre(col):
    """Pasa los nombres de columna a minúsculas y sin espacios ni símbolos."""
    return re.sub(r"[^a-z0-9]+", "_", col.strip().lower()).strip("_")


def buscar_columna(columnas, palabras, evitar=()):
    """Busca la primera columna que contenga alguna de las palabras clave."""
    for palabra in palabras:
        for c in columnas:
            if palabra in c and not any(e in c for e in evitar):
                return c
    return None


def a_numero(col):
    """Convierte texto como '1,234.5' en número."""
    return F.regexp_replace(F.col(col).cast("string"), ",", "").cast("double")


def a_fecha(col):
    """Intenta leer la fecha en varios formatos comunes."""
    texto = F.col(col).cast("string")
    return F.coalesce(
        F.to_date(texto, "yyyy-MM-dd"),
        F.to_date(texto, "yyyy-MM-dd HH:mm:ss"),
        F.to_date(texto, "M/d/yyyy"),
        F.to_date(texto, "d/M/yyyy"),
        F.to_date(texto, "yyyy-MM"),
    )


# ---------- Capa plata: limpiar y unificar todos los ductos ----------
tablas = []
for ducto, tipo in DUCTOS.items():
    ruta = f"{BASE}/raw/throughput/{ducto}-throughput-and-capacity.csv"
    try:
        df = spark.read.option("header", True).option("encoding", "UTF-8").csv(ruta)
    except Exception as error:  # si un archivo no se descargó, seguimos con los demás
        print(f"[AVISO] No se pudo leer {ducto}: {error}")
        continue

    df = df.toDF(*[limpiar_nombre(c) for c in df.columns])
    cols = df.columns

    c_fecha = "date" if "date" in cols else buscar_columna(cols, ["date", "fecha"])
    c_caudal = buscar_columna(cols, ["throughput"])
    c_capacidad = buscar_columna(cols, ["available_capacity", "capacity"], evitar=["throughput", "unused"])
    c_punto = buscar_columna(cols, ["key_point", "keypoint"])
    c_direccion = buscar_columna(cols, ["direction"])
    c_producto = buscar_columna(cols, ["product"])

    print(f"[INFO] {ducto}: fecha={c_fecha}, caudal={c_caudal}, capacidad={c_capacidad}, "
          f"punto={c_punto}, direccion={c_direccion}, producto={c_producto}")

    if not (c_caudal and c_capacidad and c_punto):
        print(f"[AVISO] {ducto}: faltan columnas clave, se omite. Columnas: {cols}")
        continue

    if c_fecha:
        fecha = a_fecha(c_fecha)
    elif "year" in cols and "month" in cols:
        fecha = F.make_date(F.col("year").cast("int"), F.col("month").cast("int"), F.lit(1))
    else:
        print(f"[AVISO] {ducto}: no hay columna de fecha, se omite.")
        continue

    tablas.append(df.select(
        F.lit(ducto).alias("ducto"),
        F.lit(tipo).alias("tipo"),
        fecha.alias("fecha"),
        F.col(c_punto).alias("punto_clave"),
        (F.col(c_direccion) if c_direccion else F.lit("sin dato")).alias("direccion"),
        (F.col(c_producto) if c_producto else F.lit("sin dato")).alias("producto"),
        a_numero(c_caudal).alias("caudal"),
        a_numero(c_capacidad).alias("capacidad"),
    ))

if not tablas:
    raise SystemExit("No se pudo procesar ningún ducto. Revisa la capa bronce.")

plata = tablas[0]
for t in tablas[1:]:
    plata = plata.unionByName(t)

plata = (plata
         .filter(F.col("fecha").isNotNull() & F.col("caudal").isNotNull())
         .withColumn("anio", F.year("fecha"))
         .withColumn("mes", F.month("fecha")))

plata.write.mode("overwrite").partitionBy("ducto", "anio").parquet(f"{BASE}/silver/throughput")
print(f"[INFO] Capa plata guardada: {plata.count()} filas")

# ---------- Capa oro: indicadores ----------
# 1) Por día y punto clave: sumamos el caudal de todos los productos;
#    la capacidad es la misma para todos los productos, así que tomamos el máximo.
diario = (plata.groupBy("ducto", "tipo", "punto_clave", "direccion", "fecha", "anio", "mes")
          .agg(F.sum("caudal").alias("caudal"), F.max("capacidad").alias("capacidad")))

# 2) Utilización mensual = caudal promedio / capacidad promedio
mensual = (diario.groupBy("ducto", "tipo", "punto_clave", "direccion", "anio", "mes")
           .agg(F.avg("caudal").alias("caudal_promedio"),
                F.avg("capacidad").alias("capacidad_promedio"))
           .withColumn("utilizacion",
                       F.when(F.col("capacidad_promedio") > 0,
                              F.col("caudal_promedio") / F.col("capacidad_promedio")))
           .withColumn("periodo", F.make_date("anio", "mes", F.lit(1))))

mensual.write.mode("overwrite").parquet(f"{BASE}/gold/utilizacion_mensual")

# 3) Resumen por ducto y año
anual = (mensual.filter(F.col("utilizacion").isNotNull())
         .groupBy("ducto", "tipo", "anio")
         .agg(F.round(F.avg("utilizacion"), 4).alias("utilizacion_promedio"),
              F.round(F.max("utilizacion"), 4).alias("utilizacion_maxima"),
              F.sum(F.when(F.col("utilizacion") >= 0.9, 1).otherwise(0)).alias("registros_sobre_90")))

anual.write.mode("overwrite").parquet(f"{BASE}/gold/resumen_anual")

print(f"[INFO] Capa oro guardada: {mensual.count()} filas mensuales, {anual.count()} filas anuales")
spark.stop()
