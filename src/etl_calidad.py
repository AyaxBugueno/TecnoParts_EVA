#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL PySpark — TecnoParts Chile (Big Data U2, Etapa 3)

raw/ (CSV, inmutable)  ->  reglas de calidad  ->  joins  ->  curated/ (Parquet particionado anio/mes)
                                              ->  results/calidad_reporte (antes/después por regla)

Uso (desde worker-1):
  spark-submit --master spark://10.0.1.10:7077,10.0.1.14:7077 --name u2_etl etl_calidad.py s3a://<bucket>
"""
import sys
from pyspark.sql import SparkSession, functions as F, types as T

BASE = sys.argv[1].rstrip("/")
if not BASE.startswith(("s3a://", "s3://", "file://")):
    BASE = f"s3a://{BASE}"
RAW, CUR, RES = f"{BASE}/raw", f"{BASE}/curated", f"{BASE}/results"
FECHA_MIN, FECHA_MAX = "2023-01-01", "2025-12-31"
SEMILLA_PARTICIONES = 36  # 3 años x 12 meses

spark = (SparkSession.builder.appName("u2_etl_calidad")
         .config("spark.sql.session.timeZone", "America/Santiago")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

S, I, L, D, TS, DB = (T.StringType(), T.IntegerType(), T.LongType(), T.DateType(),
                      T.TimestampType(), T.DoubleType())


def esquema(cols):
    return T.StructType([T.StructField(n, t, True) for n, t in cols])


ESQ = {
    "clientes": esquema([("id_cliente", I), ("rut", S), ("nombre", S), ("email", S), ("genero", S), ("edad", I),
                         ("segmento", S), ("region", S), ("comuna", S), ("es_empresa", I),
                         ("canal_preferido", S), ("fecha_registro", D)]),
    "productos": esquema([("id_producto", I), ("sku", S), ("nombre", S), ("categoria", S), ("subcategoria", S),
                          ("marca", S), ("gama", S), ("precio", I), ("costo", I), ("margen_pct", DB),
                          ("stock", I), ("rating", DB), ("activo", I), ("fecha_alta", D)]),
    "ventas": esquema([("id_venta", L), ("id_cliente", I), ("fecha_hora", TS), ("fecha", D), ("canal", S),
                       ("sucursal", S), ("medio_pago", S), ("tipo_despacho", S), ("n_items", I),
                       ("monto_neto", L), ("iva", L), ("monto_total", L), ("estado", S)]),
    "detalle_ventas": esquema([("id_detalle", L), ("id_venta", L), ("id_producto", I), ("cantidad", I),
                               ("precio_lista", I), ("descuento_pct", DB), ("precio_unitario", I),
                               ("subtotal", L)]),
    "navegacion_web": esquema([("id_evento", L), ("id_sesion", L), ("id_cliente", I), ("fecha_hora", TS),
                               ("tipo_evento", S), ("id_producto", I), ("dispositivo", S), ("navegador", S),
                               ("sistema_operativo", S), ("canal_captacion", S), ("duracion_seg", I)]),
}


def leer(tabla):
    return (spark.read.option("header", True).option("mode", "PERMISSIVE")
            .schema(ESQ[tabla]).csv(f"{RAW}/{tabla}/"))


REGIONES = ["Region Metropolitana", "Valparaiso", "Biobio", "Nuble", "Coquimbo", "Maule", "Araucania",
            "OHiggins", "Los Lagos", "Antofagasta", "Los Rios", "Tarapaca", "Atacama", "Magallanes",
            "Arica y Parinacota", "Aysen"]
EVENTOS = ["home", "search", "category_view", "product_view", "add_to_cart", "remove_from_cart",
           "checkout_start", "purchase"]

reporte = []  # (tabla, regla, clave_manifiesto, detectadas, accion)
resumen = []  # (tabla, filas_antes, filas_despues)


def medir(df, exprs):
    """Una sola pasada: devuelve dict nombre -> conteo."""
    fila = df.agg(*[F.sum(F.when(c, 1).otherwise(0)).alias(n) if n != "total" else F.count(F.lit(1)).alias(n)
                    for n, c in exprs]).first()
    return {n: int(fila[n] or 0) for n, _ in exprs}


# =============================================================== CLIENTES
cli_raw = leer("clientes").cache()
canon = F.create_map(*[x for r in REGIONES for x in (F.lit(r.lower()), F.lit(r))])
m = medir(cli_raw, [("total", F.lit(True)),
                    ("edad", (F.col("edad") < 16) | (F.col("edad") > 75)),
                    ("region", ~F.col("region").isin(REGIONES)),
                    ("email", F.col("email").isNull())])
n_cli_dist = cli_raw.dropDuplicates().count()
reporte += [("clientes", "Duplicados exactos", "clientes.duplicado_exacto", m["total"] - n_cli_dist, "descartar"),
            ("clientes", "Edad fuera de 16-75", "clientes.edad_fuera_de_rango", m["edad"], "corregir a NULL"),
            ("clientes", "Región con formato inconsistente", "clientes.region_formato_inconsistente", m["region"],
             "corregir (nombre canónico)"),
            ("clientes", "Email nulo", "clientes.email_nulo", m["email"], "conservar")]
clientes = (cli_raw.dropDuplicates()
            .withColumn("edad", F.when(F.col("edad").between(16, 75), F.col("edad")))
            .withColumn("region", F.coalesce(canon[F.lower(F.trim(F.col("region")))], F.trim(F.col("region")))))
clientes.write.mode("overwrite").parquet(f"{CUR}/clientes")
clientes = spark.read.parquet(f"{CUR}/clientes").cache()
resumen.append(("clientes", m["total"], clientes.count()))

# =============================================================== PRODUCTOS
pro_raw = leer("productos")
n_pro, n_pro_d = pro_raw.count(), pro_raw.dropDuplicates().count()
reporte.append(("productos", "Duplicados exactos", "productos.duplicado_exacto", n_pro - n_pro_d, "descartar"))
pro_raw.dropDuplicates().write.mode("overwrite").parquet(f"{CUR}/productos")
productos = spark.read.parquet(f"{CUR}/productos").cache()
resumen.append(("productos", n_pro, productos.count()))

# =============================================================== VENTAS
ven_raw = leer("ventas").cache()
ids_cli = clientes.select("id_cliente").distinct()
huerf = (ven_raw.join(F.broadcast(ids_cli), "id_cliente", "left_anti")
         .select("id_venta").distinct().count())
n_ven_raw = ven_raw.count()
fuera = F.col("fecha").isNotNull() & ((F.col("fecha") < F.lit(FECHA_MIN).cast("date")) |
                                       (F.col("fecha") > F.lit(FECHA_MAX).cast("date")))
m = medir(ven_raw, [("total", F.lit(True)), ("f_nula", F.col("fecha").isNull()), ("f_fuera", fuera)])
n_ven_dist = ven_raw.dropDuplicates().count()
n_huerf_filas = ven_raw.join(F.broadcast(ids_cli), "id_cliente", "left_anti").count()
reporte += [("ventas", "Duplicados exactos", "ventas.duplicado_exacto", n_ven_raw - n_ven_dist, "descartar"),
            ("ventas", "Fecha nula", "ventas.fecha_nula", m["f_nula"], "corregir desde fecha_hora"),
            ("ventas", "Fecha fuera de 2023-2025", "ventas.fecha_fuera_de_rango", m["f_fuera"],
             "corregir desde fecha_hora"),
            ("ventas", "id_cliente inexistente (huérfano)", "ventas.id_cliente_huerfano", n_huerf_filas,
             "descartar")]
ventas = (ven_raw.dropDuplicates()
          .withColumn("fecha", F.to_date("fecha_hora"))
          .join(F.broadcast(ids_cli), "id_cliente", "left_semi"))
fact_ventas = (ventas.join(F.broadcast(clientes.select("id_cliente", "segmento", "region", "comuna", "es_empresa",
                                                       "canal_preferido", "fecha_registro")), "id_cliente", "inner")
               .withColumn("anio", F.year("fecha")).withColumn("mes", F.month("fecha")))
(fact_ventas.repartition("anio", "mes").write.mode("overwrite").partitionBy("anio", "mes")
 .parquet(f"{CUR}/fact_ventas"))
fact_ventas = spark.read.parquet(f"{CUR}/fact_ventas").cache()
resumen.append(("ventas", n_ven_raw, fact_ventas.count()))

# =============================================================== DETALLE
det_raw = leer("detalle_ventas").cache()
n_det_raw = det_raw.count()
m = medir(det_raw, [("total", F.lit(True)), ("cant", F.col("cantidad") <= 0),
                    ("pnulo", F.col("precio_unitario").isNull())])
n_det_dist = det_raw.dropDuplicates().count()
reporte += [("detalle_ventas", "Duplicados exactos", "detalle_ventas.duplicado_exacto", n_det_raw - n_det_dist,
             "descartar"),
            ("detalle_ventas", "Cantidad <= 0", "detalle_ventas.cantidad_no_positiva", m["cant"], "descartar"),
            ("detalle_ventas", "precio_unitario nulo", "detalle_ventas.precio_unitario_nulo", m["pnulo"],
             "corregir (precio_lista x (1 - descuento))")]
det_ok = (det_raw.dropDuplicates().filter(F.col("cantidad") > 0)
          .withColumn("precio_unitario",
                      F.coalesce("precio_unitario",
                                 F.round(F.col("precio_lista") * (1 - F.col("descuento_pct") / 100)).cast("int")))
          .withColumn("subtotal", F.coalesce("subtotal", F.col("precio_unitario") * F.col("cantidad"))))
n_det_pre = det_ok.count()
ven_llave = fact_ventas.select("id_venta", "id_cliente", "fecha", "canal", "estado", "anio", "mes")
det_v = det_ok.join(ven_llave, "id_venta", "inner")
fact_detalle = det_v.join(F.broadcast(productos.select("id_producto", "categoria", "subcategoria", "marca",
                                                      "gama", "costo")), "id_producto", "inner")
(fact_detalle.repartition("anio", "mes").write.mode("overwrite").partitionBy("anio", "mes")
 .parquet(f"{CUR}/fact_detalle"))
n_det_fin = spark.read.parquet(f"{CUR}/fact_detalle").count()
reporte.append(("detalle_ventas", "Huérfanas en cascada (venta descartada)", "(sin clave en manifiesto)",
                n_det_pre - n_det_fin, "descartar"))
resumen.append(("detalle_ventas", n_det_raw, n_det_fin))

# =============================================================== NAVEGACIÓN (tabla grande: 1 sola medición)
nav_raw = leer("navegacion_web")
m = medir(nav_raw, [("total", F.lit(True)), ("f_nula", F.col("fecha_hora").isNull()),
                    ("dur_neg", F.col("duracion_seg") < 0), ("tipo", ~F.col("tipo_evento").isin(EVENTOS))])
n_nav_dist = nav_raw.select("id_evento").distinct().count()
reporte += [("navegacion_web", "Duplicados exactos", "navegacion_web.duplicado_exacto", m["total"] - n_nav_dist,
             "descartar (por id_evento)"),
            ("navegacion_web", "fecha_hora nula", "navegacion_web.fecha_hora_nula", m["f_nula"], "descartar"),
            ("navegacion_web", "duracion_seg negativa", "navegacion_web.duracion_negativa", m["dur_neg"],
             "corregir a NULL"),
            ("navegacion_web", "tipo_evento con formato inconsistente", "navegacion_web.tipo_evento_formato_inconsistente",
             m["tipo"], "corregir (trim + minúsculas)")]
nav = (nav_raw.dropDuplicates(["id_evento"]).filter(F.col("fecha_hora").isNotNull())
       .withColumn("duracion_seg", F.when(F.col("duracion_seg") >= 0, F.col("duracion_seg")))
       .withColumn("tipo_evento", F.lower(F.trim("tipo_evento")))
       .withColumn("anio", F.year("fecha_hora")).withColumn("mes", F.month("fecha_hora")))
(nav.repartition("anio", "mes").write.mode("overwrite").partitionBy("anio", "mes")
 .parquet(f"{CUR}/navegacion_web"))
resumen.append(("navegacion_web", m["total"], spark.read.parquet(f"{CUR}/navegacion_web").count()))

# =============================================================== REPORTES -> results/
try:
    man = (spark.read.option("multiLine", True).json(f"{RES}/_manifiesto.json").first()["conteo_anomalias"]).asDict()
except Exception as e:
    print("AVISO: no pude leer results/_manifiesto.json:", str(e)[:120]); man = {}

filas = []
for tabla, regla, clave, det, accion in reporte:
    esp = man.get(clave)
    filas.append((tabla, regla, accion, int(det), None if esp is None else int(esp),
                  None if esp is None else int(det) - int(esp)))
sch = T.StructType([T.StructField(n, t, True) for n, t in [
    ("tabla", S), ("regla", S), ("accion", S), ("detectadas_etl", L), ("inyectadas_manifiesto", L), ("diferencia", L)]])
rep = spark.createDataFrame(filas, sch)
res = spark.createDataFrame(resumen, ["tabla", "filas_antes", "filas_despues"]) \
    .withColumn("filas_descartadas", F.col("filas_antes") - F.col("filas_despues"))
rep.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{RES}/calidad_reporte")
res.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{RES}/calidad_resumen_tablas")

print("\n===== REPORTE DE CALIDAD (detectadas por el ETL vs manifiesto) =====")
rep.show(50, truncate=False)
print("===== FILAS ANTES / DESPUÉS POR TABLA =====")
res.show(truncate=False)
print("Diferencia > 0 = fila anómala que además fue duplicada (cuenta 2 veces en el CSV). "
      "Ver DICCIONARIO_DATOS.md sección 7.")
spark.stop()
