# U2 Big Data · Clúster Spark HA + ZooKeeper en AWS Academy

> README **preliminar (Hito 1)**: cubre Etapa 1 (infraestructura). Las secciones marcadas `TODO` se completan en las etapas 2 a 6.

## 1. Qué construye `infra/reconstruir_cluster.sh`

Un único script, ejecutado desde **AWS CloudShell (us-east-1)**, que deja operativo un clúster Spark Standalone en alta disponibilidad:

| Nodo | IP privada | Rol | Tipo |
|---|---|---|---|
| zk-1 / zk-2 / zk-3 | 10.0.1.20 / .21 / .22 | ZooKeeper (myid 1/2/3) | t3.small |
| master-1 | 10.0.1.10 | Spark Master (ALIVE al inicio) | t3.medium |
| master-2 | 10.0.1.14 | Spark Master (STANDBY) | t3.medium |
| worker-1 / 2 / 3 | 10.0.1.11 / .12 / .13 | Spark Worker (2 cores, 3 GB) | t3.medium |

- Red: VPC 10.0.0.0/16, subred 10.0.1.0/24 (us-east-1a), Internet Gateway, un Security Group (todo el tráfico entre miembros; SSH solo desde la IP /32 de CloudShell).
- Todas las instancias usan `LabInstanceProfile` y la llave `spark-key`. No hay access keys en el código.
- ZooKeeper: `zoo.cfg` (3 `server.N`, puertos 2181/2888/3888) y `myid` se generan en el user data de cada nodo.
- Spark HA: `spark-env.sh` con `-Dspark.deploy.recoveryMode=ZOOKEEPER`, `-Dspark.deploy.zookeeper.url=10.0.1.20:2181,10.0.1.21:2181,10.0.1.22:2181` y `-Dspark.deploy.zookeeper.dir=/spark`. `spark-defaults.conf` apunta a `spark://10.0.1.10:7077,10.0.1.14:7077`.

**Relación con el documento de clases** (`Documentacion_Cluster_Spark_AWS`): se reutiliza la misma estructura (VPC, SG, user data, IPs fijas, borrado por etiqueta). Cambios: 8 nodos en vez de 4, ZooKeeper + segundo master, `LabInstanceProfile`, orden de arranque y reinicio tras cerrar la sesión. **No** se usa master-1 como bastión (se detendrá en la prueba de failover): CloudShell entra por SSH a la IP pública de cada nodo.

## 2. Prerrequisitos

1. Laboratorio AWS Academy iniciado y **AWS CloudShell** abierta en `us-east-1`.
2. Cupo para 8 instancias (≈16 vCPU) y 1 VPC adicional. Si aún tiene el clúster de clases (`spark-lab`), bórrelo antes con `~/borrar-cluster.sh` (comparte el nombre de llave `spark-key`).
3. Salida a Internet desde CloudShell y desde los nodos (descarga de Spark y ZooKeeper desde apache.org).
4. Clonar este repositorio en CloudShell (el home de CloudShell persiste entre sesiones).

```bash
git clone <URL_DEL_REPO> && cd u2-bigdata-<apellido>/infra
chmod +x reconstruir_cluster.sh
```

## 3. Orden de ejecución

| Momento | Comando | Qué hace |
|---|---|---|
| Primera vez, o recursos eliminados (escenario **a**) | `./reconstruir_cluster.sh desplegar` | Crea red, llave, SG y los 8 nodos; espera el bootstrap; inicia servicios; verifica. ~8–12 min. |
| Tras cerrar la sesión del lab, instancias detenidas (escenario **b**) | `./reconstruir_cluster.sh desplegar` | Detecta nodos detenidos y los arranca **por grupos: ZooKeeper → masters → workers** (esperando chequeos de estado en cada grupo); reescribe las IP públicas en `~/.ssh/config`; re-autoriza SSH para la IP actual de CloudShell; reinicia los procesos Spark/ZooKeeper (no arrancan solos). ~4–6 min. |
| Al terminar cada sesión de trabajo | `./reconstruir_cluster.sh detener` | Detiene workers → masters → ZooKeeper (ahorra créditos). |
| Para empezar de cero | `./reconstruir_cluster.sh borrar` | Elimina instancias, VPC y llave del proyecto. |

El mismo comando `desplegar` es **idempotente**: crea solo lo que falta, arranca solo lo detenido y no toca lo que ya corre. Si un nodo fue borrado, se recrea con su misma IP privada.

Otros comandos: `estado`, `verificar`, `webui <IP>`, `userdata <nodo>` (imprime el user data que recibe un nodo, sin usar AWS).

Orden de arranque de servicios dentro de `desplegar`: ZooKeeper (se espera 1 leader + 2 followers) → `start-master.sh` en master-1 (ALIVE) → master-2 (STANDBY) → `start-worker.sh spark://10.0.1.10:7077,10.0.1.14:7077` en los 3 workers.

## 4. Verificación

Automática: `./reconstruir_cluster.sh verificar` (termina con código ≠ 0 si algo falla).

Manual (evidencias):

```bash
# E9 · ensemble: debe haber 1 leader y 2 followers
for z in zk-1 zk-2 zk-3; do
  echo "== $z"; ssh $z 'JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 /opt/zookeeper/bin/zkServer.sh status 2>&1 | grep Mode'
done

# E1/E2 · estado de los masters y workers vivos (también por navegador, ver §5)
for m in master-1 master-2; do
  echo "== $m"; ssh $m 'curl -s localhost:8080/json/ | python3 -m json.tool | grep -E "\"status\"|aliveworkers"'
done
```

Resultado esperado: `Mode: leader` (1) y `Mode: follower` (2); master-1 `ALIVE` con `aliveworkers: 3` y master-2 `STANDBY`.

## 5. WebUI desde su navegador (sin 0.0.0.0/0)

1. Abra <https://checkip.amazonaws.com> **en el navegador** y copie su IP.
2. `./reconstruir_cluster.sh webui <SU_IP>` (abre 8080-8081 y 4040 solo para esa IP /32).
3. IP pública de cada nodo: `./reconstruir_cluster.sh estado`. Abra `http://<IP_master-1>:8080` (E1) y `http://<IP_master-2>:8080` (E2).

Cada captura debe mostrar URL/host y la hora del sistema (ver enunciado).

## 6. Seguridad y buenas prácticas

- El `.pem` se crea en `~/.ssh/spark-key.pem` (permisos 400) y **nunca** se versiona: `.gitignore` excluye `*.pem`, credenciales y datos.
- SSH (22) solo desde la IP /32 de CloudShell; el script retira reglas de IP antiguas en cada ejecución.
- Decisión de laboratorio: las IP públicas se reasignan al reiniciar, por eso `~/.ssh/config` usa `StrictHostKeyChecking no` y `UserKnownHostsFile /dev/null` solo para estos hosts (no apto para producción).
- Los procesos Spark/ZooKeeper no arrancan solos tras un reinicio: siempre usar `desplegar`.

## 7. Versiones

| Componente | Versión | Nota |
|---|---|---|
| Spark | 3.5.0 (`-bin-hadoop3`) | Variable `SPARK_VERSION`. |
| ZooKeeper | 3.9.5 | Variable `ZK_VERSION`; si no estuviera publicada, usar 3.8.6. |
| Java / SO | OpenJDK 17 / Ubuntu 22.04 | |
| hadoop-aws | `TODO (Etapa 2)` | Debe coincidir con el Hadoop del build de Spark (`ls $SPARK_HOME/jars \| grep hadoop-client`). |

## 8. Problemas frecuentes

| Síntoma | Qué hacer |
|---|---|
| `ExpiredToken` | La sesión del lab venció: reiniciar el lab y abrir CloudShell nueva. |
| `spark-key existe en AWS pero falta el .pem` | `borrar` y luego `desplegar` (AWS no permite recuperar la llave privada). |
| Un nodo no termina el bootstrap | `ssh <nodo> 'sudo tail -n 40 /var/log/cloud-init-output.log'` (descarga de Spark/ZooKeeper o `apt`). |
| SSH con timeout | Cambió la IP de CloudShell o el nodo está detenido: volver a ejecutar `desplegar`. |
| `DependencyViolation` al borrar | Esperar 1–2 min y repetir `borrar`. |
| `InstanceLimitExceeded` | Cupo del lab insuficiente: consultar al docente o bajar tipos con `SPARK_TYPE=t3.small`. |

## 9. Estructura del repositorio

```
u2-bigdata-<apellido>/
├── README.md               # este archivo
├── requirements.txt        # TODO: versiones de numpy/pandas del generador
├── infra/reconstruir_cluster.sh
├── src/etl_*.py            # TODO Etapa 3
├── src/modelo_*.py         # TODO Etapa 4
├── evidencias/             # E1–E13
├── planificacion/          # Gantt H1 y final + retrospectiva
├── DECLARACION_IA.md       # obligatorio
└── .gitignore
```
