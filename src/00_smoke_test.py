"""Prueba de humo: clúster HA + S3A con instance profile (sin access keys).
Uso: spark-submit --master spark://10.0.1.10:7077,10.0.1.14:7077 00_smoke_test.py <bucket>
"""
import sys
from pyspark.sql import SparkSession

bucket = sys.argv[1]
spark = SparkSession.builder.appName("u2_smoke_test").getOrCreate()

# 1) Lectura S3A
n = spark.read.option("header", True).csv(f"s3a://{bucket}/raw/clientes/").count()
print(f"[SMOKE] filas en raw/clientes: {n}")

# 2) Cómputo distribuido (fuerza tareas en todos los executors)
total = spark.range(0, 200_000_000, numPartitions=12).selectExpr("sum(id) AS s").first()["s"]
print(f"[SMOKE] suma distribuida: {total}")

# 3) Escritura S3A
spark.createDataFrame([("ok", n, int(total))], ["estado", "filas_clientes", "suma"]) \
     .coalesce(1).write.mode("overwrite").json(f"s3a://{bucket}/results/smoke_test/")
print("[SMOKE] escritura en results/smoke_test OK")
spark.stop()
