#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
===============================================================================
GENERADOR DE DATASET SINTÉTICO — RETAIL TECNOLÓGICO "TecnoParts Chile"
Big Data · Unidad 2 Arquitectura de Big Data · Evaluación sumativa N.º 2
===============================================================================
Genera 5 CSV relacionados y coherentes entre sí (esquema estrella + clickstream):

    clientes.csv · productos.csv · ventas.csv · detalle_ventas.csv · navegacion_web.csv

y un manifiesto `_manifiesto.json` (parámetros, filas, bytes, md5 y anomalías
inyectadas) que sirve como evidencia E11 y para la verificación docente.

USO
  python3 generador_dataset.py --seed 5678                 # evaluación (~2,5 GB)
  python3 generador_dataset.py --seed 5678 --gb 0.1        # desarrollo (~100 MB)
  python3 generador_dataset.py --seed 5678 --verificar     # docente: conteos y md5
                                                           # sin escribir (rápido)
  --seed  : ÚLTIMOS 4 DÍGITOS DEL RUT SIN DV (obligatorio). 0371 -> --seed 371
  --gb    : tamaño total aproximado en GB decimales (1 GB = 10^9 bytes).
            La entrega final se genera con el valor por defecto (2.5).
  --out   : carpeta de salida (por defecto ./dataset)
  --sin-anomalias : no inyecta problemas de calidad (solo para pruebas)

REPRODUCIBILIDAD
  * Cada tabla y cada bloque de clientes usa su propio flujo aleatorio
    numpy.random.RandomState (API legacy, con flujo congelado entre versiones
    de NumPy), derivado de (seed, tabla, bloque). Misma semilla + mismo --gb
    => mismos archivos en cualquier equipo.
  * clientes, productos, ventas y detalle_ventas NO dependen del clickstream:
    el modo --verificar los regenera en segundos para comparar conteos y md5.
  * NO modifique las constantes marcadas "NO MODIFICAR": cambian los datos.

REQUISITOS: Python >= 3.9, numpy >= 1.21, pandas >= 1.5
MEMORIA: procesa por bloques de clientes; pico aprox. 1 GB de RAM.
DISCO: --gb + 10 % (la escritura es secuencial, sin archivos temporales).
===============================================================================
"""
import argparse
import hashlib
import io
import json
import os
import sys
import time
from datetime import date

import numpy as np
import pandas as pd

VERSION = "2.0"

# ============================ PARÁMETROS (NO MODIFICAR) ======================
FECHA_INICIO = np.datetime64("2023-01-01")
FECHA_FIN = np.datetime64("2025-12-31")          # inclusive (3 años completos)
N_PRODUCTOS = 2000
CLIENTES_POR_GB = 45_100                         # calibrado: --gb 2.5 ~ 2,50 GB (113 mil clientes)
BLOQUE_CLIENTES = 2_000                          # tamaño de bloque (afecta los datos)
SESIONES_NO_CONV_POR_CONV = 31                   # tasa de conversión ~3 %
IVA = 0.19
TASAS_ANOMALIA = {                               # proporción de filas afectadas
    "duplicado_exacto": 0.002,
    "nulo": 0.0005,
    "fuera_de_rango": 0.001,
    "huerfano": 0.0005,
    "formato": 0.001,
}
# =============================================================================

DIAS = int((FECHA_FIN - FECHA_INICIO).astype(int)) + 1
TABLAS = ["clientes", "productos", "ventas", "detalle_ventas", "navegacion_web"]
_TID = {t: i for i, t in enumerate(TABLAS)}
_TID["anomalias"] = 9


def rng_para(seed, tabla, bloque=0):
    """Flujo aleatorio independiente y estable por (semilla, tabla, bloque)."""
    s = (seed * 1_000_003 + _TID[tabla] * 7_919 + bloque * 104_729 + 12_345) % (2**32 - 1)
    return np.random.RandomState(s)


# --------------------------------------------------------------- catálogos
NOMBRES_M = ["Juan", "Pedro", "Diego", "Matias", "Sebastian", "Felipe", "Cristobal", "Ignacio",
             "Vicente", "Benjamin", "Tomas", "Nicolas", "Joaquin", "Rodrigo", "Gabriel", "Martin",
             "Francisco", "Jose", "Manuel", "Alonso", "Camilo", "Bastian", "Maximiliano", "Agustin"]
NOMBRES_F = ["Maria", "Camila", "Francisca", "Valentina", "Sofia", "Antonia", "Josefa", "Isidora",
             "Fernanda", "Constanza", "Javiera", "Catalina", "Emilia", "Florencia", "Trinidad",
             "Amanda", "Martina", "Agustina", "Monserrat", "Paula", "Daniela", "Carolina", "Andrea"]
APELLIDOS = ["Gonzalez", "Munoz", "Rojas", "Diaz", "Perez", "Soto", "Contreras", "Silva", "Martinez",
             "Sepulveda", "Morales", "Rodriguez", "Lopez", "Fuentes", "Hernandez", "Torres", "Araya",
             "Flores", "Espinoza", "Valenzuela", "Castillo", "Tapia", "Reyes", "Gutierrez", "Vergara",
             "Ledezma", "Nunez", "Fernandez", "Carrasco", "Vega", "Ramirez", "Alvarez", "Vasquez"]
DOMINIOS = ["gmail.com", "outlook.com", "hotmail.com", "yahoo.com", "empresa.cl"]

# región: (comunas, peso poblacional aprox., sucursal física más cercana)
REGIONES = {
    "Region Metropolitana": (["Santiago", "Maipu", "Puente Alto", "La Florida", "Las Condes",
                              "Nunoa", "Providencia", "Penalolen", "Quilicura", "San Bernardo"], 0.40, "Santiago Centro"),
    "Valparaiso": (["Valparaiso", "Vina del Mar", "Quilpue", "Villa Alemana", "San Antonio"], 0.10, "Vina del Mar"),
    "Biobio": (["Concepcion", "Talcahuano", "Los Angeles", "Coronel"], 0.08, "Concepcion"),
    "Nuble": (["Chillan", "San Carlos"], 0.03, "Concepcion"),
    "Coquimbo": (["La Serena", "Coquimbo", "Ovalle", "Illapel", "Vicuna"], 0.05, "La Serena"),
    "Maule": (["Talca", "Curico", "Linares", "Cauquenes"], 0.06, "Concepcion"),
    "Araucania": (["Temuco", "Padre Las Casas", "Villarrica", "Angol"], 0.05, "Temuco"),
    "OHiggins": (["Rancagua", "San Fernando", "Rengo", "Machali"], 0.05, "Santiago Centro"),
    "Los Lagos": (["Puerto Montt", "Osorno", "Castro", "Ancud"], 0.05, "Temuco"),
    "Antofagasta": (["Antofagasta", "Calama", "Tocopilla"], 0.04, "La Serena"),
    "Los Rios": (["Valdivia", "La Union", "Rio Bueno"], 0.02, "Temuco"),
    "Tarapaca": (["Iquique", "Alto Hospicio"], 0.02, "La Serena"),
    "Atacama": (["Copiapo", "Vallenar"], 0.02, "La Serena"),
    "Magallanes": (["Punta Arenas", "Puerto Natales"], 0.01, "Temuco"),
    "Arica y Parinacota": (["Arica"], 0.01, "La Serena"),
    "Aysen": (["Coyhaique", "Puerto Aysen"], 0.01, "Temuco"),
}
REG_NOMBRES = list(REGIONES)
REG_P = np.array([REGIONES[r][1] for r in REG_NOMBRES]); REG_P /= REG_P.sum()
SUCURSALES = ["Santiago Centro", "Providencia", "Vina del Mar", "Concepcion", "La Serena", "Temuco"]

SEGMENTOS = ["Gamer", "Corporativo", "Estudiante", "Profesional TI", "Hogar"]
SEG_P = np.array([0.22, 0.18, 0.20, 0.15, 0.25])
SEG_EDAD = np.array([26, 40, 21, 34, 38])

# subcategoría: (categoría, precio_min, precio_max, marcas)
CATALOGO = {
    "Procesador (CPU)": ("Componentes", 80000, 700000, ["Intel", "AMD"]),
    "Tarjeta de Video (GPU)": ("Componentes", 150000, 2500000, ["NVIDIA", "ASUS", "MSI", "Gigabyte", "Zotac", "PNY"]),
    "Placa Madre": ("Componentes", 55000, 600000, ["ASUS", "MSI", "Gigabyte", "ASRock"]),
    "Memoria RAM": ("Componentes", 22000, 300000, ["Kingston", "Corsair", "G.Skill", "Crucial", "ADATA"]),
    "Almacenamiento SSD": ("Componentes", 18000, 260000, ["Kingston", "Samsung", "Crucial", "WD", "ADATA"]),
    "Almacenamiento HDD": ("Componentes", 25000, 160000, ["Seagate", "WD", "Toshiba"]),
    "Fuente de Poder": ("Componentes", 28000, 260000, ["Corsair", "EVGA", "Cooler Master", "Thermaltake"]),
    "Gabinete": ("Componentes", 22000, 220000, ["Cooler Master", "NZXT", "Corsair", "Thermaltake"]),
    "Refrigeracion": ("Componentes", 9000, 190000, ["Cooler Master", "Noctua", "Corsair", "DeepCool"]),
    "Notebook": ("Computadores", 320000, 3800000, ["HP", "Dell", "Lenovo", "ASUS", "Acer", "MSI", "Apple"]),
    "PC Armado": ("Computadores", 420000, 4200000, ["TecnoParts", "XtremeGamer", "Corsair"]),
    "Monitor": ("Monitores", 58000, 1300000, ["Samsung", "LG", "ASUS", "AOC", "Acer", "Dell"]),
    "Teclado": ("Perifericos", 7000, 190000, ["Logitech", "Razer", "HyperX", "Redragon", "Corsair"]),
    "Mouse": ("Perifericos", 5000, 130000, ["Logitech", "Razer", "HyperX", "Redragon"]),
    "Audifonos": ("Perifericos", 10000, 420000, ["HyperX", "Razer", "Logitech", "Sony", "JBL"]),
    "Webcam": ("Perifericos", 15000, 160000, ["Logitech", "Razer", "Microsoft"]),
    "Impresora": ("Impresion", 40000, 520000, ["HP", "Epson", "Canon", "Brother"]),
    "Router": ("Redes", 20000, 320000, ["TP-Link", "Asus", "Netgear", "Mercusys"]),
    "Switch": ("Redes", 25000, 420000, ["TP-Link", "Cisco", "Netgear", "D-Link"]),
    "Access Point": ("Redes", 30000, 350000, ["Ubiquiti", "TP-Link", "Cisco"]),
    "Smartphone": ("Celulares", 100000, 1800000, ["Samsung", "Xiaomi", "Motorola", "Apple", "Honor"]),
    "Tablet": ("Celulares", 90000, 1200000, ["Samsung", "Lenovo", "Apple", "Xiaomi"]),
    "Consola": ("Gaming", 250000, 800000, ["Sony", "Microsoft", "Nintendo"]),
    "Almacenamiento Externo": ("Almacenamiento", 30000, 300000, ["WD", "Seagate", "Samsung", "Kingston"]),
    "Accesorios": ("Accesorios", 3000, 60000, ["Genius", "Logitech", "Klip Xtreme", "Generico"]),
}
SUBCATS = list(CATALOGO)
GAMAS = ["Gama Entrada", "Gama Media", "Gama Alta", "Premium"]

AFINIDAD = {  # peso relativo segmento -> subcategoría (resto = 0.4)
    "Gamer": {"Tarjeta de Video (GPU)": 5, "Procesador (CPU)": 4, "Placa Madre": 3, "Memoria RAM": 3,
              "Monitor": 4, "Teclado": 3, "Mouse": 3, "Audifonos": 3, "Gabinete": 2, "Refrigeracion": 2,
              "Fuente de Poder": 2, "Consola": 3, "Almacenamiento SSD": 2, "PC Armado": 3},
    "Corporativo": {"Notebook": 5, "Monitor": 4, "Impresora": 4, "Router": 3, "Switch": 3, "Access Point": 3,
                    "Teclado": 2, "Mouse": 2, "PC Armado": 2, "Webcam": 2, "Almacenamiento Externo": 2},
    "Estudiante": {"Notebook": 5, "Tablet": 4, "Audifonos": 3, "Mouse": 3, "Almacenamiento Externo": 3,
                   "Smartphone": 3, "Accesorios": 3, "Almacenamiento SSD": 2, "Teclado": 2},
    "Profesional TI": {"Notebook": 4, "Procesador (CPU)": 3, "Memoria RAM": 3, "Almacenamiento SSD": 4,
                       "Monitor": 4, "Router": 3, "Switch": 3, "Access Point": 3, "Placa Madre": 2,
                       "Tarjeta de Video (GPU)": 2},
    "Hogar": {"Smartphone": 4, "Tablet": 3, "Impresora": 3, "Teclado": 2, "Mouse": 2, "Accesorios": 4,
              "Consola": 3, "Audifonos": 3, "Monitor": 2, "Notebook": 2, "Router": 2, "Almacenamiento Externo": 2},
}

CANALES = ["Web", "App", "Tienda", "Telefonico"]
CANAL_PREF_P = [0.55, 0.20, 0.20, 0.05]
DESPACHOS = ["Despacho Domicilio", "Retiro en Tienda", "Despacho Express"]
MEDIOS = ["Webpay Credito", "Webpay Debito", "Transferencia", "Mercado Pago", "Efectivo", "Cuotas Tienda"]
MEDIO_P = {  # por canal (Efectivo y Cuotas Tienda solo existen en tienda física)
    "Web": [0.38, 0.27, 0.18, 0.17, 0, 0], "App": [0.36, 0.30, 0.10, 0.24, 0, 0],
    "Tienda": [0.30, 0.30, 0.0, 0.0, 0.20, 0.20], "Telefonico": [0.40, 0.0, 0.60, 0.0, 0, 0],
}
ESTADOS = ["Completada", "Anulada", "Devuelta"]

DISPOSITIVOS = ["Desktop", "Mobile", "Tablet"]
SO_POR_DISP = {"Desktop": (["Windows", "macOS", "Linux"], [0.70, 0.20, 0.10]),
               "Mobile": (["Android", "iOS"], [0.68, 0.32]),
               "Tablet": (["Android", "iOS"], [0.45, 0.55])}
NAV_POR_SO = {"Windows": (["Chrome", "Edge", "Firefox", "Opera"], [0.62, 0.26, 0.09, 0.03]),
              "macOS": (["Safari", "Chrome", "Firefox"], [0.60, 0.35, 0.05]),
              "Linux": (["Chrome", "Firefox"], [0.50, 0.50]),
              "Android": (["Chrome", "Firefox", "Opera"], [0.85, 0.08, 0.07]),
              "iOS": (["Safari", "Chrome"], [0.80, 0.20])}
CAPTACION = ["Directo", "Google Organico", "Google Ads", "Facebook", "Instagram", "Email", "YouTube"]
EVENTOS = ["home", "search", "category_view", "product_view", "add_to_cart",
           "remove_from_cart", "checkout_start", "purchase"]
E = {e: i for i, e in enumerate(EVENTOS)}
DUR = {"home": (3, 40), "search": (5, 60), "category_view": (5, 60), "product_view": (10, 150),
       "add_to_cart": (3, 20), "remove_from_cart": (2, 12), "checkout_start": (20, 120), "purchase": (5, 30)}


# ----------------------------------------------------- calendario comercial
def factor_diario():
    """Multiplicador de demanda por día (tendencia + estacionalidad + campañas
    simuladas). Las ventanas de campaña son APROXIMADAS: no son las fechas
    oficiales de cada año."""
    d = FECHA_INICIO + np.arange(DIAS)
    m = d.astype("datetime64[M]").astype(int) % 12 + 1
    dia = (d - d.astype("datetime64[M]")).astype(int) + 1
    dow = (d.astype(int) + 3) % 7                       # 0 = lunes
    f = 1.25 ** (np.arange(DIAS) / 365.0)               # crecimiento ~25 %/año
    f *= np.select([m == 1, m == 2, m == 3, m == 7, m == 12],
                   [0.80, 0.85, 1.30, 1.15, 1.35], 1.0)  # verano bajo, clases, vacaciones, Navidad
    f *= np.where((m == 12) & (dia >= 25), 0.55, 1.0)    # caída post-Navidad
    cyberday = (m == 6) & (dia <= 7) & (dow <= 2)        # 1.ª semana de junio, lun-mié
    cybermon = (((m == 9) & (dia >= 28)) | ((m == 10) & (dia <= 8))) & (dow <= 2)
    bfriday = (m == 11) & (dia >= 24) & (dow >= 4)       # último fin de semana largo de nov.
    f *= np.where(cyberday, 2.6, 1.0) * np.where(cybermon, 2.3, 1.0) * np.where(bfriday, 1.9, 1.0)
    f *= np.array([0.85, 0.90, 0.95, 1.05, 1.10, 1.15, 1.00])[dow]
    camp = cyberday | cybermon | bfriday
    return f, camp


FACTOR, CAMPANA = factor_diario()
CDF_DIA = np.concatenate([[0.0], np.cumsum(FACTOR)]); CDF_DIA /= CDF_DIA[-1]


def dias_estacionales(rng, ini, fin):
    """Día (offset desde FECHA_INICIO) en [ini, fin] con prob. proporcional a FACTOR."""
    lo, hi = CDF_DIA[ini], CDF_DIA[fin + 1]
    u = lo + rng.random_sample(len(ini)) * (hi - lo)
    return np.clip(np.searchsorted(CDF_DIA, u, side="right") - 1, ini, fin)


def elegir(rng, opciones, p, n):
    return np.asarray(opciones, dtype=object)[rng.choice(len(opciones), size=n, p=np.asarray(p) / np.sum(p))]


def muestrear_ponderado(rng, cdf, n):
    return np.minimum(np.searchsorted(cdf, rng.random_sample(n), side="right"), len(cdf) - 1)


# ------------------------------------------------------------- productos
def generar_productos(seed):
    rng = rng_para(seed, "productos")
    sub = np.asarray(SUBCATS, dtype=object)[rng.randint(0, len(SUBCATS), N_PRODUCTOS)]
    cat = np.array([CATALOGO[s][0] for s in sub], dtype=object)
    marca = np.array([CATALOGO[s][3][rng.randint(len(CATALOGO[s][3]))] for s in sub], dtype=object)
    pmin = np.array([CATALOGO[s][1] for s in sub], float)
    pmax = np.array([CATALOGO[s][2] for s in sub], float)
    q = rng.random_sample(N_PRODUCTOS)                 # posición en el rango (log-uniforme)
    precio = np.exp(np.log(pmin) + q * (np.log(pmax) - np.log(pmin)))
    precio = (np.round(precio / 1000) * 1000 - 10).astype(np.int64)      # terminación ...990
    gama_idx = np.minimum((q * 4).astype(int), 3)       # gama coherente con el precio
    margen = np.round(rng.uniform(0.08, 0.35, N_PRODUCTOS) * 100, 1)
    costo = np.round(precio * (1 - margen / 100)).astype(np.int64)
    sufijo = np.asarray(["", "Pro", "Plus", "Max", "X", "G", "RGB"], dtype=object)[rng.randint(0, 7, N_PRODUCTOS)]
    ids = np.arange(1, N_PRODUCTOS + 1)
    nombre = [f"{m} {s} {GAMAS[g]} {x}".strip() for m, s, g, x in zip(marca, sub, gama_idx, sufijo)]
    sku = [f"{s[:3].upper()}-{m[:3].upper().replace('.', '')}-{i:05d}" for s, m, i in zip(sub, marca, ids)]
    alta = FECHA_INICIO - rng.randint(0, 400, N_PRODUCTOS).astype("timedelta64[D]")
    df = pd.DataFrame({
        "id_producto": ids, "sku": sku, "nombre": nombre, "categoria": cat, "subcategoria": sub,
        "marca": marca, "gama": np.asarray(GAMAS, dtype=object)[gama_idx], "precio": precio, "costo": costo,
        "margen_pct": margen, "stock": rng.randint(0, 350, N_PRODUCTOS),
        "rating": np.round(np.clip(rng.normal(4.2, 0.5, N_PRODUCTOS), 2.5, 5.0), 1),
        "activo": (rng.random_sample(N_PRODUCTOS) > 0.05).astype(int),
        "fecha_alta": np.datetime_as_string(alta, unit="D"),
    })
    pop = rng.pareto(1.3, N_PRODUCTOS) + 1                # popularidad latente (ley de potencia)
    pop /= pop.sum()
    cdf_pop = np.cumsum(pop); cdf_pop /= cdf_pop[-1]
    cdf_seg = []
    for s in SEGMENTOS:
        w = np.array([AFINIDAD[s].get(x, 0.4) for x in sub]) * pop
        c = np.cumsum(w); cdf_seg.append(c / c[-1])
    return df, precio, cdf_pop, cdf_seg


# --------------------------------------------------- clientes (por bloque)
def generar_clientes(seed, b, id0, n):
    rng = rng_para(seed, "clientes", b)
    ids = np.arange(id0, id0 + n, dtype=np.int64)
    genero = np.where(rng.random_sample(n) < 0.5, "M", "F").astype(object)
    nm = np.asarray(NOMBRES_M, dtype=object)[rng.randint(0, len(NOMBRES_M), n)]
    nf = np.asarray(NOMBRES_F, dtype=object)[rng.randint(0, len(NOMBRES_F), n)]
    nombre1 = np.where(genero == "M", nm, nf)
    ap1 = np.asarray(APELLIDOS, dtype=object)[rng.randint(0, len(APELLIDOS), n)]
    ap2 = np.asarray(APELLIDOS, dtype=object)[rng.randint(0, len(APELLIDOS), n)]
    seg = rng.choice(len(SEGMENTOS), size=n, p=SEG_P)
    reg = rng.choice(len(REG_NOMBRES), size=n, p=REG_P)
    comuna = np.array([REGIONES[REG_NOMBRES[r]][0][rng.randint(len(REGIONES[REG_NOMBRES[r]][0]))] for r in reg], dtype=object)
    edad = np.clip(np.round(rng.normal(SEG_EDAD[seg], 8)), 16, 75).astype(int)
    es_emp = np.where(seg == 1, rng.random_sample(n) < 0.70, rng.random_sample(n) < 0.03).astype(int)
    canal_pref = rng.choice(4, size=n, p=CANAL_PREF_P)
    # registro sesgado hacia el inicio del periodo; deja al menos 30 días de vida
    freg = (np.floor((DIAS - 30) * (1 - np.sqrt(rng.random_sample(n))))).astype(int)
    lam = np.clip(rng.gamma(1.6, 1.4, n), 0.2, 12.0) * np.where(es_emp == 1, 1.6, 1.0)  # órdenes/año
    churn = rng.random_sample(n) < 0.35
    vida = DIAS - 1 - freg
    fin = np.where(churn, freg + np.maximum(5, (vida * rng.uniform(0.15, 0.7, n)).astype(int)), DIAS - 1)
    # RUT único: biyección afín sobre [5.000.000, 25.000.000) + DV módulo 11
    num = 5_000_000 + (15_485_863 * ids + 7_777 * (seed % 1000) + 1) % 20_000_000
    dv = rut_dv(num)
    rut = [f"{x:,}".replace(",", ".") + "-" + d for x, d in zip(num, dv)]
    email = [f"{a.lower()}.{p.lower()}{k}@{d}" for a, p, k, d in
             zip(nombre1, ap1, rng.randint(1, 999, n), np.where(es_emp == 1, "empresa.cl",
                 np.asarray(DOMINIOS[:4], dtype=object)[rng.randint(0, 4, n)]))]
    df = pd.DataFrame({
        "id_cliente": ids, "rut": rut, "nombre": nombre1 + " " + ap1 + " " + ap2, "email": email,
        "genero": genero, "edad": edad, "segmento": np.asarray(SEGMENTOS, dtype=object)[seg],
        "region": np.asarray(REG_NOMBRES, dtype=object)[reg], "comuna": comuna, "es_empresa": es_emp,
        "canal_preferido": np.asarray(CANALES, dtype=object)[canal_pref],
        "fecha_registro": np.datetime_as_string(FECHA_INICIO + freg.astype("timedelta64[D]"), unit="D"),
    })
    lat = dict(seg=seg, reg=reg, es_emp=es_emp, canal_pref=canal_pref, freg=freg, fin=fin, lam=lam)
    return df, lat


def rut_dv(num):
    s = np.zeros_like(num); m = 2; x = num.copy()
    for _ in range(8):
        s += (x % 10) * m; x //= 10; m = 2 if m == 7 else m + 1
    r = 11 - s % 11
    return np.where(r == 11, "0", np.where(r == 10, "K", r.astype(str)))


# ------------------------------------------- ventas + detalle (por bloque)
def generar_ventas(seed, b, lat, ids_cli, id_venta0, id_det0, precio, cdf_seg):
    rng = rng_para(seed, "ventas", b)
    n = len(ids_cli)
    activos = lat["fin"] - lat["freg"]
    n_ord = np.minimum(rng.poisson(lat["lam"] * activos / 365.0), 80)
    ci = np.repeat(np.arange(n), n_ord)                  # índice local del cliente por orden
    nv = len(ci)
    dia = dias_estacionales(rng, lat["freg"][ci] + 1, lat["fin"][ci])   # siempre > registro
    hora = np.clip(np.round(rng.normal(16, 4, nv)), 0, 23).astype(int)
    seg_s = rng.randint(0, 3600, nv)
    ts = (FECHA_INICIO + dia.astype("timedelta64[D]")).astype("datetime64[s]") + \
        (hora * 3600 + seg_s).astype("timedelta64[s]")
    canal = np.where(rng.random_sample(nv) < 0.65, lat["canal_pref"][ci], rng.choice(4, size=nv, p=CANAL_PREF_P))
    canal_s = np.asarray(CANALES, dtype=object)[canal]
    # sucursal: tienda más cercana a la región del cliente (85 %), otra (15 %)
    suc_cercana = np.array([REGIONES[REG_NOMBRES[r]][2] for r in lat["reg"][ci]], dtype=object)
    suc = np.where(rng.random_sample(nv) < 0.85, suc_cercana,
                   np.asarray(SUCURSALES, dtype=object)[rng.randint(0, len(SUCURSALES), nv)])
    suc = np.where(canal == 2, suc, "Online")
    medio = np.empty(nv, dtype=object)
    for k, c in enumerate(CANALES):
        mk = canal == k
        medio[mk] = elegir(rng, MEDIOS, MEDIO_P[c], mk.sum())
    emp = lat["es_emp"][ci] == 1
    medio = np.where(emp & (rng.random_sample(nv) < 0.5) & (canal != 2), "Transferencia", medio)
    despacho = np.where(canal == 2,
                        np.where(rng.random_sample(nv) < 0.85, "Retiro en Tienda", "Despacho Domicilio"),
                        elegir(rng, DESPACHOS, [0.60, 0.25, 0.15], nv))
    estado = elegir(rng, ESTADOS, [0.90, 0.06, 0.04], nv)
    # ---- líneas: ítems pedidos con reposición, luego distintos por orden
    k_items = np.clip(rng.geometric(0.55, nv), 1, 6)
    oi = np.repeat(np.arange(nv), k_items)
    seg_o = lat["seg"][ci][oi]
    prod = np.empty(len(oi), dtype=np.int64)
    for s in range(len(SEGMENTOS)):
        ms = seg_o == s
        prod[ms] = muestrear_ponderado(rng, cdf_seg[s], ms.sum()) + 1
    lin = pd.DataFrame({"o": oi, "p": prod}).drop_duplicates()
    oi, prod = lin["o"].to_numpy(), lin["p"].to_numpy()
    nl = len(oi)
    emp_l = emp[oi]
    cant = np.where(emp_l, np.clip(rng.geometric(0.40, nl), 1, 10), np.clip(rng.geometric(0.70, nl), 1, 5))
    camp = CAMPANA[dia]
    d_ord = np.where(camp, elegir(rng, [0, 0.05, 0.10, 0.15, 0.20, 0.25], [0.25, 0.15, 0.25, 0.18, 0.12, 0.05], nv),
                     elegir(rng, [0, 0.05, 0.10], [0.75, 0.15, 0.10], nv)).astype(float)
    d_lin = elegir(rng, [0, 0.05, 0.10], [0.75, 0.17, 0.08], nl).astype(float)
    desc = np.minimum(0.35, d_ord[oi] + d_lin)
    plista = precio[prod - 1]
    punit = np.round(plista * (1 - desc)).astype(np.int64)
    subtotal = punit * cant
    n_items = np.bincount(oi, minlength=nv)
    neto = np.bincount(oi, weights=subtotal, minlength=nv).astype(np.int64)
    iva = np.round(neto * IVA).astype(np.int64)
    id_venta = np.arange(id_venta0, id_venta0 + nv)
    ventas = pd.DataFrame({
        "id_venta": id_venta, "id_cliente": ids_cli[ci], "fecha_hora": ts,
        "fecha": np.datetime_as_string(ts.astype("datetime64[D]"), unit="D"), "canal": canal_s,
        "sucursal": suc, "medio_pago": medio, "tipo_despacho": despacho, "n_items": n_items,
        "monto_neto": neto, "iva": iva, "monto_total": neto + iva, "estado": estado,
    })
    detalle = pd.DataFrame({
        "id_detalle": np.arange(id_det0, id_det0 + nl), "id_venta": id_venta[oi], "id_producto": prod,
        "cantidad": cant, "precio_lista": plista, "descuento_pct": np.round(desc * 100, 1),
        "precio_unitario": punit, "subtotal": subtotal,
    })
    aux = dict(ci=ci, ts=ts, canal=canal, estado=estado, oi=oi, prod=prod, dia=dia)
    return ventas, detalle, aux


# ------------------------------------------------ clickstream (por bloque)
def atributos_tecnicos(rng, n, app_mask=None):
    disp = rng.choice(3, size=n, p=[0.45, 0.45, 0.10])
    if app_mask is not None:
        disp = np.where(app_mask, 1, disp)
    so = np.empty(n, dtype=object); nav = np.empty(n, dtype=object)
    for k, d in enumerate(DISPOSITIVOS):
        mk = disp == k
        so[mk] = elegir(rng, *SO_POR_DISP[d], mk.sum())
    for s, (ops, p) in NAV_POR_SO.items():
        mk = so == s
        nav[mk] = elegir(rng, ops, p, mk.sum())
    if app_mask is not None:
        nav = np.where(app_mask, "App", nav)
    cap = elegir(rng, CAPTACION, [0.25, 0.28, 0.14, 0.10, 0.09, 0.08, 0.06], n)
    return np.asarray(DISPOSITIVOS, dtype=object)[disp], so, nav, cap


def generar_navegacion(seed, b, lat, ids_cli, ventas, aux, id_ses0, id_ev0, cdf_pop, cdf_seg):
    rng = rng_para(seed, "navegacion_web", b)
    # --- sesiones que convierten: una por venta Web/App completada
    conv = np.flatnonzero(np.isin(aux["canal"], [0, 1]) & (aux["estado"] == "Completada"))
    nc = len(conv)
    nn = int(round(nc * SESIONES_NO_CONV_POR_CONV))
    ns = nc + nn
    es_conv = np.r_[np.ones(nc, bool), np.zeros(nn, bool)]
    # cliente de la sesión: conversión -> comprador; no conversión -> 45 % logueado
    cli_local = np.full(ns, -1)
    cli_local[:nc] = aux["ci"][conv]
    logueado_nc = rng.random_sample(nn) < 0.45
    peso = lat["lam"] * (lat["fin"] - lat["freg"] + 30)
    cdf_cli = np.cumsum(peso) / peso.sum()
    cli_local[nc:][logueado_nc] = muestrear_ponderado(rng, cdf_cli, logueado_nc.sum())
    # día de la sesión no convertida: logueado -> dentro de su vida activa (+30 días);
    # anónimo -> cualquier día con estacionalidad
    dia = np.empty(ns, dtype=np.int64)
    dia[:nc] = aux["dia"][conv]
    cl = cli_local[nc:]
    lo = np.where(cl >= 0, lat["freg"][np.maximum(cl, 0)], 0)
    hi = np.where(cl >= 0, np.minimum(lat["fin"][np.maximum(cl, 0)] + 30, DIAS - 1), DIAS - 1)
    dia[nc:] = dias_estacionales(rng, lo, hi)
    # --- composición de cada sesión (conteos por tipo de evento)
    home = rng.random_sample(ns) < 0.80
    n_search = rng.poisson(np.where(es_conv, 0.9, 0.35))
    n_cat = rng.poisson(np.where(es_conv, 0.7, 0.55))
    n_view_extra = np.where(es_conv, rng.poisson(1.6, ns), np.clip(rng.geometric(0.42, ns), 1, 15) - 1)
    rebote = (~es_conv) & (rng.random_sample(ns) < 0.30)          # una sola página
    n_view_extra = np.where(rebote, 0, n_view_extra)
    n_cat = np.where(rebote, 0, n_cat); n_search = np.where(rebote, 0, n_search)
    home = np.where(rebote, True, home)
    # productos comprados por sesión de conversión
    lineas_ord = pd.Series(aux["prod"]).groupby(aux["oi"]).apply(np.asarray)
    compra_prods = [lineas_ord[o] for o in conv]
    n_buy = np.r_[np.array([len(x) for x in compra_prods], dtype=np.int64), np.zeros(nn, np.int64)]
    # ---- eventos: (sesión, clave de orden, tipo, producto)
    S, K, T, P = [], [], [], []

    def add(ses, key, tipo, prod):
        S.append(ses); K.append(key); T.append(np.full(len(ses), E[tipo], np.int8)); P.append(prod)

    sid = np.arange(ns)
    h = sid[home]; add(h, np.full(len(h), -1.0), "home", np.full(len(h), -1))
    s_ = np.repeat(sid, n_search); add(s_, rng.random_sample(len(s_)), "search", np.full(len(s_), -1))
    c_ = np.repeat(sid, n_cat); add(c_, rng.random_sample(len(c_)), "category_view", np.full(len(c_), -1))
    # vistas de productos "extra": logueado -> afinidad de su segmento; anónimo -> popularidad
    v_ = np.repeat(sid, n_view_extra)
    segv = np.where(cli_local[v_] >= 0, lat["seg"][np.maximum(cli_local[v_], 0)], -1)
    pv = muestrear_ponderado(rng, cdf_pop, len(v_)) + 1
    for s in range(len(SEGMENTOS)):
        ms = segv == s
        pv[ms] = muestrear_ponderado(rng, cdf_seg[s], ms.sum()) + 1
    kv = rng.random_sample(len(v_))
    add(v_, kv, "product_view", pv)
    # carritos abandonados en vistas extra (8 %), algunos luego removidos
    cart = rng.random_sample(len(v_)) < np.where(es_conv[v_], 0.06, 0.08)
    add(v_[cart], kv[cart] + 1e-6, "add_to_cart", pv[cart])
    rem = cart & (rng.random_sample(len(v_)) < 0.35)
    add(v_[rem], kv[rem] + 2e-6, "remove_from_cart", pv[rem])
    # productos comprados: vista + carrito (en sesiones de conversión)
    if nc:
        b_ = np.repeat(np.arange(nc), n_buy[:nc])
        pb = np.concatenate(compra_prods)
        kb = rng.random_sample(len(b_))
        add(b_, kb, "product_view", pb)
        add(b_, kb + 1e-6, "add_to_cart", pb)
    # checkout: conversión siempre; no conversión con carrito -> 25 % inicia y abandona
    tiene_carro = np.zeros(ns, bool); tiene_carro[v_[cart]] = True
    chk = es_conv | (tiene_carro & (rng.random_sample(ns) < 0.25))
    ck = sid[chk]; add(ck, np.full(len(ck), 2.0), "checkout_start", np.full(len(ck), -1))
    pu = sid[:nc]; add(pu, np.full(nc, 3.0), "purchase", np.full(nc, -1))
    ses = np.concatenate(S); key = np.concatenate(K); tipo = np.concatenate(T); prod = np.concatenate(P)
    orden = np.lexsort((key, ses))
    ses, tipo, prod = ses[orden], tipo[orden], prod[orden]
    ne = len(ses)
    # duración por evento y marcas de tiempo acumuladas dentro de la sesión
    lo_d = np.array([DUR[e][0] for e in EVENTOS])[tipo]
    hi_d = np.array([DUR[e][1] for e in EVENTOS])[tipo]
    dur = lo_d + (rng.random_sample(ne) * (hi_d - lo_d + 1)).astype(int)
    dur = np.where(es_conv[ses], (dur * 1.25).astype(int), dur)   # compradores leen más
    inicio_ses = np.r_[0, np.flatnonzero(np.diff(ses)) + 1]
    csum = np.cumsum(dur) - dur
    offset = csum - np.repeat(csum[inicio_ses], np.diff(np.r_[inicio_ses, ne]))
    # inicio de sesión: conversión -> se ajusta para que 'purchase' coincida con la venta
    hora = np.clip(np.round(rng.normal(15, 5, ns)), 0, 23).astype(int)
    t0 = (FECHA_INICIO + dia.astype("timedelta64[D]")).astype("datetime64[s]") + \
        (hora * 3600 + rng.randint(0, 3600, ns)).astype("timedelta64[s]")
    if nc:
        es_pur = tipo == E["purchase"]
        off_pur = np.zeros(ns, np.int64); off_pur[ses[es_pur]] = offset[es_pur]
        t0[:nc] = aux["ts"][conv] - off_pur[:nc].astype("timedelta64[s]")
    ts = t0[ses] + offset.astype("timedelta64[s]")
    # identidad: 25 % de los compradores navega anónimo y se identifica en el checkout
    login_tardio = np.r_[rng.random_sample(nc) < 0.25, np.zeros(nn, bool)]
    cli_ev = cli_local[ses]
    id_cli = np.where(cli_ev >= 0, ids_cli[np.maximum(cli_ev, 0)], -1)
    anon = login_tardio[ses] & (tipo < E["checkout_start"])
    id_cli = np.where(anon, -1, id_cli)
    # atributos técnicos por sesión (App -> Mobile + "App")
    app = np.zeros(ns, bool)
    app[:nc] = aux["canal"][conv] == 1
    app[nc:] = rng.random_sample(nn) < 0.18
    disp, so, navg, cap = atributos_tecnicos(rng, ns, app)
    df = pd.DataFrame({
        "id_evento": np.arange(id_ev0, id_ev0 + ne),
        "id_sesion": (id_ses0 + ses),
        "id_cliente": pd.array(np.where(id_cli < 0, 0, id_cli), dtype="Int64"),
        "fecha_hora": ts, "tipo_evento": np.asarray(EVENTOS, dtype=object)[tipo], "id_producto": prod,
        "dispositivo": disp[ses], "navegador": navg[ses], "sistema_operativo": so[ses],
        "canal_captacion": cap[ses], "duracion_seg": dur,
    })
    df.loc[id_cli < 0, "id_cliente"] = pd.NA
    return df, ns


# ------------------------------------------------------- anomalías de calidad
class Anomalias:
    def __init__(self, seed, activo):
        self.activo = activo; self.seed = seed; self.conteo = {}

    def _n(self, clave, n):
        self.conteo[clave] = self.conteo.get(clave, 0) + int(n)

    def _idx(self, rng, n, tasa):
        k = rng.binomial(n, tasa) if n else 0
        return rng.choice(n, size=k, replace=False) if k else np.array([], int)

    def duplicar(self, rng, df, tabla):
        i = self._idx(rng, len(df), TASAS_ANOMALIA["duplicado_exacto"])
        self._n(f"{tabla}.duplicado_exacto", len(i))
        if not len(i):
            return df
        orden = np.sort(np.r_[np.arange(len(df)), i], kind="stable")   # duplicado junto al original
        return df.iloc[orden].reset_index(drop=True)

    def aplicar(self, tabla, df, b, max_id_cliente):
        if not self.activo:
            return df
        rng = rng_para(self.seed, "anomalias", b * 10 + _TID[tabla])
        n = len(df)
        if tabla == "clientes":
            i = self._idx(rng, n, TASAS_ANOMALIA["fuera_de_rango"])
            df.loc[i, "edad"] = rng.choice([0, 3, 120, 150, 999], size=len(i)); self._n("clientes.edad_fuera_de_rango", len(i))
            i = self._idx(rng, n, TASAS_ANOMALIA["formato"])
            df.loc[i, "region"] = [f"  {r.upper()} " if k % 2 else f"{r.lower()} " for k, r in enumerate(df.loc[i, "region"])]
            self._n("clientes.region_formato_inconsistente", len(i))
            i = self._idx(rng, n, TASAS_ANOMALIA["nulo"])
            df.loc[i, "email"] = None; self._n("clientes.email_nulo", len(i))
        elif tabla == "productos":
            pass                                          # solo duplicados
        elif tabla == "ventas":
            i = self._idx(rng, n, TASAS_ANOMALIA["nulo"])
            df.loc[i, "fecha"] = None; self._n("ventas.fecha_nula", len(i))
            i = self._idx(rng, n, TASAS_ANOMALIA["fuera_de_rango"])
            df.loc[i, "fecha"] = rng.choice(["1999-01-01", "2035-07-15", "2030-12-31"], size=len(i))
            self._n("ventas.fecha_fuera_de_rango", len(i))
            i = self._idx(rng, n, TASAS_ANOMALIA["huerfano"])
            df.loc[i, "id_cliente"] = max_id_cliente + 1 + rng.randint(0, 10_000_000, size=len(i))
            self._n("ventas.id_cliente_huerfano", len(i))
        elif tabla == "detalle_ventas":
            i = self._idx(rng, n, TASAS_ANOMALIA["fuera_de_rango"])
            df.loc[i, "cantidad"] = rng.choice([0, -1, -2], size=len(i)); self._n("detalle_ventas.cantidad_no_positiva", len(i))
            i = self._idx(rng, n, TASAS_ANOMALIA["nulo"])
            df["precio_unitario"] = df["precio_unitario"].astype("Int64")
            df.loc[i, "precio_unitario"] = pd.NA; self._n("detalle_ventas.precio_unitario_nulo", len(i))
        elif tabla == "navegacion_web":
            i = self._idx(rng, n, TASAS_ANOMALIA["nulo"])
            df.loc[i, "fecha_hora"] = pd.NaT; self._n("navegacion_web.fecha_hora_nula", len(i))
            i = self._idx(rng, n, TASAS_ANOMALIA["fuera_de_rango"])
            df.loc[i, "duracion_seg"] = -1; self._n("navegacion_web.duracion_negativa", len(i))
            i = self._idx(rng, n, TASAS_ANOMALIA["formato"])
            df.loc[i, "tipo_evento"] = [f" {t.upper()}" for t in df.loc[i, "tipo_evento"]]
            self._n("navegacion_web.tipo_evento_formato_inconsistente", len(i))
        return self.duplicar(rng, df, tabla)


# ---------------------------------------------------------------- escritura
class Salida:
    """Escribe CSV por bloques (o solo calcula md5/bytes en modo verificación)."""

    def __init__(self, carpeta, tabla, escribir):
        self.tabla, self.escribir = tabla, escribir
        self.md5, self.bytes, self.filas, self.primero = hashlib.md5(), 0, 0, True
        self.f = open(os.path.join(carpeta, f"{tabla}.csv"), "wb") if escribir else None

    def agregar(self, df):
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=self.primero, lineterminator="\n",
                  float_format="%.1f", date_format="%Y-%m-%d %H:%M:%S")
        data = buf.getvalue().encode("utf-8")
        self.md5.update(data); self.bytes += len(data); self.filas += len(df); self.primero = False
        if self.f:
            self.f.write(data)

    def cerrar(self):
        if self.f:
            self.f.close()
        return {"filas": self.filas, "bytes": self.bytes, "md5": self.md5.hexdigest()}


# --------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description="Generador de dataset retail (Big Data U2)")
    ap.add_argument("--seed", type=int, required=True, help="últimos 4 dígitos del RUT sin DV")
    ap.add_argument("--gb", type=float, default=2.5, help="tamaño total aproximado en GB (defecto 2.5)")
    ap.add_argument("--out", default="./dataset", help="carpeta de salida")
    ap.add_argument("--sin-anomalias", action="store_true", help="no inyectar problemas de calidad")
    ap.add_argument("--verificar", action="store_true",
                    help="no escribe; calcula filas y md5 de clientes/productos/ventas/detalle")
    a = ap.parse_args()
    if not 0 <= a.seed <= 9999:
        sys.exit("ERROR: --seed debe ser un entero entre 0 y 9999 (últimos 4 dígitos del RUT sin DV).")
    if not 0.01 <= a.gb <= 20:
        sys.exit("ERROR: --gb debe estar entre 0.01 y 20.")
    n_cli = max(BLOQUE_CLIENTES // 4, int(round(CLIENTES_POR_GB * a.gb)))
    escribir = not a.verificar
    tablas = TABLAS if escribir else TABLAS[:4]
    if escribir:
        os.makedirs(a.out, exist_ok=True)
    t_ini = time.time()
    print(f"Generador v{VERSION} | seed={a.seed} | gb={a.gb} | clientes={n_cli:,} | "
          f"{'VERIFICACIÓN (sin escribir)' if a.verificar else 'salida=' + os.path.abspath(a.out)}")

    out = {t: Salida(a.out, t, escribir) for t in tablas}
    anom = Anomalias(a.seed, not a.sin_anomalias)
    productos, precio, cdf_pop, cdf_seg = generar_productos(a.seed)
    out["productos"].agregar(anom.aplicar("productos", productos.copy(), 0, n_cli))

    id_venta, id_det, id_ses, id_ev = 1, 1, 1, 1
    n_bloques = (n_cli + BLOQUE_CLIENTES - 1) // BLOQUE_CLIENTES
    for b in range(n_bloques):
        id0 = b * BLOQUE_CLIENTES + 1
        nb = min(BLOQUE_CLIENTES, n_cli - id0 + 1)
        cli, lat = generar_clientes(a.seed, b, id0, nb)
        ids_cli = cli["id_cliente"].to_numpy()
        ven, det, aux = generar_ventas(a.seed, b, lat, ids_cli, id_venta, id_det, precio, cdf_seg)
        assert det["id_venta"].isin(ven["id_venta"]).all() and ven["id_cliente"].isin(ids_cli).all()
        if escribir:
            nav, ns = generar_navegacion(a.seed, b, lat, ids_cli, ven, aux, id_ses, id_ev, cdf_pop, cdf_seg)
            id_ses += ns; id_ev += len(nav)
            out["navegacion_web"].agregar(anom.aplicar("navegacion_web", nav, b, n_cli))
            del nav
        id_venta += len(ven); id_det += len(det)
        out["clientes"].agregar(anom.aplicar("clientes", cli, b, n_cli))
        out["ventas"].agregar(anom.aplicar("ventas", ven, b, n_cli))
        out["detalle_ventas"].agregar(anom.aplicar("detalle_ventas", det, b, n_cli))
        if b % 5 == 0 or b == n_bloques - 1:
            tot = sum(o.bytes for o in out.values())
            print(f"  bloque {b + 1:>3}/{n_bloques}  {tot / 1e9:6.2f} GB  {time.time() - t_ini:6.0f} s", flush=True)

    res = {t: o.cerrar() for t, o in out.items()}
    total = sum(r["bytes"] for r in res.values())
    manifiesto = {
        "generador_version": VERSION, "seed": a.seed, "gb_solicitado": a.gb, "n_clientes": n_cli,
        "periodo": [str(FECHA_INICIO), str(FECHA_FIN)], "anomalias_inyectadas": not a.sin_anomalias,
        "modo": "verificacion" if a.verificar else "completo",
        "numpy": np.__version__, "pandas": pd.__version__, "python": sys.version.split()[0],
        "fecha_generacion": date.today().isoformat(), "segundos": round(time.time() - t_ini, 1),
        "bytes_total": total, "tablas": res, "conteo_anomalias": dict(sorted(anom.conteo.items())),
    }
    print("\n=== RESUMEN ===")
    for t, r in res.items():
        print(f"{t:16s} {r['filas']:>12,} filas {r['bytes'] / 1e6:10.1f} MB  md5 {r['md5']}")
    print(f"{'TOTAL':16s} {'':>12} {total / 1e9:13.3f} GB  ({time.time() - t_ini:.0f} s)")
    if escribir:
        with open(os.path.join(a.out, "_manifiesto.json"), "w", encoding="utf-8") as f:
            json.dump(manifiesto, f, indent=2, ensure_ascii=False)
        print(f"Manifiesto: {os.path.join(a.out, '_manifiesto.json')}")
    else:
        print("Modo verificación: compare filas y md5 con el _manifiesto.json del estudiante.")


if __name__ == "__main__":
    main()
