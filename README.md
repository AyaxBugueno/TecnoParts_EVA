# U2 Big Data — TecnoParts Chile (Spark Standalone HA + ZooKeeper + S3)

**Estudiante:** _completar_ · **RUT:** _completar_ · **Semilla:** _completar (últimos 4 dígitos sin DV)_ · **Ruta de modelo:** _A / B_

## Versiones
Spark 3.5.0 (Hadoop 3.3.4) · hadoop-aws 3.3.4 + aws-java-sdk-bundle 1.12.262 · ZooKeeper 3.8.4 · Ubuntu 22.04 · OpenJDK 17 · Python 3.10 · numpy 1.26.4 · pandas 2.2.2

Verificación hadoop-aws ↔ Hadoop del build de Spark (en cualquier nodo Spark):
```bash
ls /opt/spark/jars | grep -E "hadoop-client|hadoop-aws|aws-java-sdk"
# hadoop-client-api-3.3.4.jar  hadoop-client-runtime-3.3.4.jar  hadoop-aws-3.3.4.jar  aws-java-sdk-bundle-1.12.262.jar
```

## 1. Prerrequisitos
- Sesión activa de AWS Academy (Learner Lab, us-east-1) y **AWS CloudShell** abierto desde esa consola.
- Rol `LabRole` / instance profile `LabInstanceProfile` (existen por defecto en Academy; no se crean roles).
- Tu IP pública (la de tu computador, no la de CloudShell): abre https://checkip.amazonaws.com en tu navegador.
- `generador_dataset.py` (extraído del notebook del curso con `infra/extraer_generador.py`).
- **Nunca** subir `spark-key.pem` ni credenciales: el .pem vive en `~/spark-key.pem` de CloudShell, fuera del repo (`.gitignore`).

## 2. Escenario (a): construir desde cero
```bash
git clone <este-repo> && cd u2-bigdata/infra        # o sube la carpeta a CloudShell
export APELLIDO=<apellido> SEMILLA=<ultimos4> MI_IP=<tu.ip.publica>
./reconstruir_cluster.sh crear
```
Hace, en orden: key pair `spark-key` → VPC 10.0.0.0/16 + subred 10.0.1.0/24 + IGW + SG (WebUI/ssh solo a tu IP /32) → bucket
`u2-bigdata-<apellido>-<semilla>` con `raw/ curated/ results/` → 8 instancias con IP privada fija y `LabInstanceProfile`
(zk-1..3 = .20/.21/.22 · master-1 .10 · master-2 .14 · worker-1..3 .11/.12/.13) → instalación por user-data
(ZooKeeper `zoo.cfg` + `myid`; Spark con `spark-env.sh`: `spark.deploy.recoveryMode=ZOOKEEPER` y los 3 ZooKeeper) → arranque
**ZooKeeper → master-1 → master-2 → workers** → verificación.

## 3. Escenario (b): instancias detenidas al cerrar el laboratorio
Las IP privadas no cambian; las públicas sí. Las reglas del SG se actualizan solas con tu IP actual.
```bash
export APELLIDO=<apellido> SEMILLA=<ultimos4> MI_IP=<tu.ip.publica>
./reconstruir_cluster.sh arrancar      # start-instances en orden ZK -> masters -> workers + servicios
./reconstruir_cluster.sh verificar
```
Al terminar cada sesión: `./reconstruir_cluster.sh detener`.

## 4. Verificación
```bash
./reconstruir_cluster.sh verificar     # E9: 1 leader + 2 followers · masters ALIVE/STANDBY · 3 workers ALIVE
# manual en un nodo ZK:  /opt/zookeeper/bin/zkServer.sh status
# WebUI (tu navegador):  http://<IP pública master-1>:8080  /  http://<IP pública master-2>:8080  /  worker :8081
```

## 5. Dataset y data lake
```bash
python3 infra/extraer_generador.py Generador_Dataset.ipynb      # produce generador_dataset.py
./reconstruir_cluster.sh datos     # genera en worker-1 (--seed $SEMILLA --gb 2.5), sube a s3://$BUCKET/raw/<tabla>/<tabla>.csv,
                                   # y deja _manifiesto.json en el directorio actual (E11)
aws s3 ls s3://$BUCKET/raw/ --recursive --human-readable --summarize     # E8
```
`raw/` es inmutable: el ETL solo escribe en `curated/` y `results/`.

## 6. Acceso S3A (instance profile, sin access keys)
`conf/spark-defaults.conf` (en cada nodo) usa `fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider`.
No hay access keys en código ni archivos.

## 7. Ejecutar aplicaciones (driver en worker-1, NO en CloudShell ni master-1)
```bash
./reconstruir_cluster.sh prueba    # humo: S3A + 3 workers
# aplicaciones reales (desde worker-1):
/opt/spark/bin/spark-submit --master spark://10.0.1.10:7077,10.0.1.14:7077 --name u2_etl ~/src/etl_*.py <bucket>
```
Justificación: en modo client el driver corre donde se lanza `spark-submit` y los executors deben conectarse de vuelta a él;
CloudShell está fuera de la VPC y master-1 será detenido en el failover.

## 8. Prueba de failover (E1–E7)
1. Captura E1 (master-1 ALIVE, 3 workers) y E2 (master-2 STANDBY), con hora visible.
2. Lanza un trabajo largo en worker-1 (`spark-submit ... --name u2_modelo`).
3. E3, en master-1: `date; /opt/spark/sbin/stop-master.sh; date`
4. En master-2 (E5): `grep -E "elected leader|Recovery|ALIVE|workers" /opt/spark/logs/*Master*.out | tail -30` → tiempo de recuperación = hora detención − hora "I have been elected leader".
5. E4: WebUI master-2 → ALIVE, 3 workers ALIVE, aplicación visible. Lanza otro `spark-submit` y confirma que termina (E6).
6. E7 (pestaña Executors / detalle de la app) y E10 (worker :8081).
7. Restaurar: en master-1 `/opt/spark/sbin/start-master.sh` (volverá como STANDBY).

## 9. Estructura
`infra/` script de reconstrucción · `src/` ETL y modelo · `evidencias/` E1–E13 · `planificacion/` Anexo C · `results/` salidas · `DECLARACION_IA.md`
