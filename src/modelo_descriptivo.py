#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Modelo Descriptivo / Analítico — TecnoParts Chile (Big Data U2, Etapa 4)
Calcula métricas de negocio por región y segmento desde curated/fact_ventas
y guarda los resultados en results/modelo_resumen_segmentos/
"""
import sys
from pyspark.sql import SparkSession, functions as F

BASE = sys.argv[1].rstrip("/")
if not BASE.startswith(("s3a://", "s3://", "file://")):
    BASE = f"s3a://{BASE}"

CUR, RES = f"{BASE}/curated", f"{BASE}/results"

spark = (SparkSession.builder.appName("u2_modelo_descriptivo")
         .config("spark.sql.session.timeZone", "America/Santiago")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

print("--- Leyendo fact_ventas desde curated/ ---")
df_ventas = spark.read.parquet(f"{CUR}/fact_ventas")

print("--- Ejecutando agregación analítica por Región y Segmento ---")
resumen_segmentos = (
    df_ventas.groupBy("region", "segmento")
    .agg(
        F.count("id_venta").alias("total_ventas"),
        F.sum("monto_total").alias("ingreso_total"),
        F.avg("monto_total").alias("ticket_promedio"),
        F.sum("n_items").alias("total_items_vendidos")
    )
    .orderBy(F.desc("ingreso_total"))
)

resumen_segmentos.show(20, truncate=False)

print("--- Guardando resultados en results/modelo_resumen_segmentos ---")
resumen_segmentos.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{RES}/modelo_resumen_segmentos")

print("¡Modelo descriptivo ejecutado y guardado con éxito!")
spark.stop()
