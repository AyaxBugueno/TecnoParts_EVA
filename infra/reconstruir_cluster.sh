#!/usr/bin/env bash
# =============================================================================
# reconstruir_cluster.sh — Big Data U2 · TecnoParts Chile
# Crea / arranca / verifica / detiene el clúster Spark Standalone HA + ZooKeeper
# en AWS Academy (us-east-1). Ejecutar DESDE CLOUDSHELL.
#
# USO
#   export APELLIDO=ledezma SEMILLA=5678 MI_IP=200.1.2.3     # MI_IP = IP pública de TU computador
#   ./reconstruir_cluster.sh crear      # escenario (a): crea todo (o arranca lo que ya existe)
#   ./reconstruir_cluster.sh arrancar   # escenario (b): instancias detenidas -> ZK -> masters -> workers
#   ./reconstruir_cluster.sh verificar  # ZK (1 leader + 2 followers), masters, workers
#   ./reconstruir_cluster.sh datos      # genera dataset en worker-1 y lo sube a s3://BUCKET/raw/
#   ./reconstruir_cluster.sh prueba     # spark-submit de humo (HA + S3A) desde worker-1
#   ./reconstruir_cluster.sh estado     # lista nodos, estado e IPs
#   ./reconstruir_cluster.sh detener    # stop-instances (al terminar cada sesión)
#   ./reconstruir_cluster.sh destruir   # elimina instancias y red (pide confirmación)
#
# NUNCA subir al repositorio el .pem (se crea en ~/spark-key.pem, fuera del repo).
# =============================================================================
set -euo pipefail
export AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""

# ----------------------------- parámetros ------------------------------------
APELLIDO=${APELLIDO:?Define APELLIDO (ej: export APELLIDO=ledezma)}
SEMILLA=${SEMILLA:?Define SEMILLA = últimos 4 dígitos del RUT sin DV (0371 -> 371)}
MI_IP=${MI_IP:-}                      # IP pública de tu computador (para WebUI 8080/8081/4040 y ssh)
GB=${GB:-2.5}                         # 0.1 para pruebas; 2.5 = entrega final
BUCKET=${BUCKET:-u2-bigdata-${APELLIDO,,}-${SEMILLA}}
AZ=${AZ:-us-east-1a}
KEY_NAME=spark-key
KEY_FILE=$HOME/${KEY_NAME}.pem
PROFILE=LabInstanceProfile
TIPO_ZK=${TIPO_ZK:-t3.micro}
TIPO_MASTER=${TIPO_MASTER:-t3.small}
TIPO_WORKER=${TIPO_WORKER:-t3.large}  # 8 GB RAM: cabe ejecutor + driver en worker-1
SPARK_V=3.5.0
ZK_V=3.8.4
HADOOP_AWS_V=3.3.4                    # debe coincidir con el Hadoop de Spark 3.5.0 (hadoop-client-api-3.3.4)
AWS_SDK_V=1.12.262                    # SDK v1 que corresponde a hadoop-aws 3.3.4

declare -A IP=([zk-1]=10.0.1.20 [zk-2]=10.0.1.21 [zk-3]=10.0.1.22
               [master-1]=10.0.1.10 [master-2]=10.0.1.14
               [worker-1]=10.0.1.11 [worker-2]=10.0.1.12 [worker-3]=10.0.1.13)
ORDEN=(zk-1 zk-2 zk-3 master-1 master-2 worker-1 worker-2 worker-3)
ZKL="${IP[zk-1]}:2181,${IP[zk-2]}:2181,${IP[zk-3]}:2181"
MASTERS="${IP[master-1]}:7077,${IP[master-2]}:7077"
declare -A PUB=()

SSH_OPTS="-n -i $KEY_FILE -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"
# Spark muestra en la WebUI el link público de cada nodo (la IP pública cambia en cada arranque)
PUBDNS='T=$(curl -s -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 60"); export SPARK_PUBLIC_DNS=$(curl -s -H "X-aws-ec2-metadata-token: $T" http://169.254.169.254/latest/meta-data/public-ipv4);'

die() { echo "ERROR: $*" >&2; exit 1; }
tag() { echo "ResourceType=$1,Tags=[{Key=Name,Value=$2},{Key=Proyecto,Value=u2-bigdata}]"; }
rssh() { local n=$1; shift; ssh $SSH_OPTS "ubuntu@${PUB[$n]}" "$@"; }
nada() { [[ -z "$1" || "$1" == "None" ]]; }

# ----------------------------- red -------------------------------------------
red() {
  echo ">> Red (VPC 10.0.0.0/16, subred 10.0.1.0/24, IGW)"
  VPC=$(aws ec2 describe-vpcs --filters Name=tag:Name,Values=u2-vpc --query 'Vpcs[0].VpcId' --output text)
  if nada "$VPC"; then
    VPC=$(aws ec2 create-vpc --cidr-block 10.0.0.0/16 --tag-specifications "$(tag vpc u2-vpc)" --query Vpc.VpcId --output text)
    aws ec2 modify-vpc-attribute --vpc-id "$VPC" --enable-dns-hostnames
  fi
  IGW=$(aws ec2 describe-internet-gateways --filters Name=attachment.vpc-id,Values="$VPC" --query 'InternetGateways[0].InternetGatewayId' --output text)
  if nada "$IGW"; then
    IGW=$(aws ec2 create-internet-gateway --tag-specifications "$(tag internet-gateway u2-igw)" --query InternetGateway.InternetGatewayId --output text)
    aws ec2 attach-internet-gateway --internet-gateway-id "$IGW" --vpc-id "$VPC"
  fi
  SUBNET=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC" Name=cidr-block,Values=10.0.1.0/24 --query 'Subnets[0].SubnetId' --output text)
  if nada "$SUBNET"; then
    SUBNET=$(aws ec2 create-subnet --vpc-id "$VPC" --cidr-block 10.0.1.0/24 --availability-zone "$AZ" --tag-specifications "$(tag subnet u2-subnet)" --query Subnet.SubnetId --output text)
    aws ec2 modify-subnet-attribute --subnet-id "$SUBNET" --map-public-ip-on-launch
  fi
  RT=$(aws ec2 describe-route-tables --filters Name=vpc-id,Values="$VPC" Name=tag:Name,Values=u2-rt --query 'RouteTables[0].RouteTableId' --output text)
  if nada "$RT"; then
    RT=$(aws ec2 create-route-table --vpc-id "$VPC" --tag-specifications "$(tag route-table u2-rt)" --query RouteTable.RouteTableId --output text)
    aws ec2 create-route --route-table-id "$RT" --destination-cidr-block 0.0.0.0/0 --gateway-id "$IGW" >/dev/null
    aws ec2 associate-route-table --route-table-id "$RT" --subnet-id "$SUBNET" >/dev/null
  fi
  SG=$(aws ec2 describe-security-groups --filters Name=vpc-id,Values="$VPC" Name=group-name,Values=u2-sg --query 'SecurityGroups[0].GroupId' --output text)
  if nada "$SG"; then
    SG=$(aws ec2 create-security-group --group-name u2-sg --description "u2 bigdata cluster" --vpc-id "$VPC" --tag-specifications "$(tag security-group u2-sg)" --query GroupId --output text)
    aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol -1 --source-group "$SG" >/dev/null   # tráfico interno libre
  fi
  refrescar_sg
}

# Reglas externas: se recrean en cada ejecución porque la IP de CloudShell y la tuya cambian.
refrescar_sg() {
  local cs old
  cs=$(curl -s https://checkip.amazonaws.com | tr -d '[:space:]')
  old=$(aws ec2 describe-security-groups --group-ids "$SG" --query 'SecurityGroups[0].IpPermissions[?length(IpRanges)>`0`]' --output json)
  if [[ "$old" != "[]" ]]; then aws ec2 revoke-security-group-ingress --group-id "$SG" --ip-permissions "$old" >/dev/null; fi
  aws ec2 authorize-security-group-ingress --group-id "$SG" --ip-permissions \
    "IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=$cs/32,Description=cloudshell-ssh}]" >/dev/null
  if [[ -n "$MI_IP" ]]; then
    aws ec2 authorize-security-group-ingress --group-id "$SG" --ip-permissions \
      "IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=$MI_IP/32,Description=mi-ip-ssh}]" \
      "IpProtocol=tcp,FromPort=8080,ToPort=8081,IpRanges=[{CidrIp=$MI_IP/32,Description=mi-ip-webui}]" \
      "IpProtocol=tcp,FromPort=4040,ToPort=4045,IpRanges=[{CidrIp=$MI_IP/32,Description=mi-ip-appui}]" >/dev/null
  else
    echo "AVISO: MI_IP vacío -> WebUI 8080/8081/4040 no accesibles desde tu navegador."
  fi
  echo "   SG $SG: ssh desde CloudShell $cs/32 ; WebUI desde ${MI_IP:-<nadie>}/32"
}

bucket() {
  echo ">> Bucket s3://$BUCKET con raw/ curated/ results/"
  if ! aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
    aws s3api create-bucket --bucket "$BUCKET" >/dev/null
  fi
  for p in raw curated results; do aws s3api put-object --bucket "$BUCKET" --key "$p/" >/dev/null; done
}

llave() {
  if ! aws ec2 describe-key-pairs --key-names "$KEY_NAME" >/dev/null 2>&1; then
    echo ">> Creando key pair $KEY_NAME -> $KEY_FILE"
    aws ec2 create-key-pair --key-name "$KEY_NAME" --query KeyMaterial --output text > "$KEY_FILE"
    chmod 400 "$KEY_FILE"
  elif [[ ! -f "$KEY_FILE" ]]; then
    die "La key $KEY_NAME existe en AWS pero no tienes $KEY_FILE. Cópiala a esa ruta (chmod 400) o bórrala: aws ec2 delete-key-pair --key-name $KEY_NAME"
  fi
}

# ----------------------------- user-data (bootstrap de cada nodo) ------------
ud_base() { cat <<'EOF'
#!/bin/bash
exec > /var/log/bootstrap.log 2>&1
set -ex
export DEBIAN_FRONTEND=noninteractive
timedatectl set-timezone America/Santiago
apt-get update -y
apt-get install -y openjdk-17-jre-headless python3-pip curl unzip
EOF
}

ud_zk() {
  local id=$1
  ud_base
  cat <<EOF
curl -fsSL --retry 5 -o /tmp/zk.tgz https://archive.apache.org/dist/zookeeper/zookeeper-$ZK_V/apache-zookeeper-$ZK_V-bin.tar.gz
tar xzf /tmp/zk.tgz -C /opt
ln -sfn /opt/apache-zookeeper-$ZK_V-bin /opt/zookeeper
mkdir -p /var/lib/zookeeper
echo $id > /var/lib/zookeeper/myid
cat > /opt/zookeeper/conf/zoo.cfg <<'CFG'
tickTime=2000
initLimit=10
syncLimit=5
dataDir=/var/lib/zookeeper
clientPort=2181
admin.enableServer=false
4lw.commands.whitelist=srvr,stat,ruok,mntr
server.1=${IP[zk-1]}:2888:3888
server.2=${IP[zk-2]}:2888:3888
server.3=${IP[zk-3]}:2888:3888
CFG
echo 'export JVMFLAGS="-Xmx256m"' > /opt/zookeeper/conf/java.env
echo 'export PATH=\$PATH:/opt/zookeeper/bin' > /etc/profile.d/zk.sh
cat > /etc/systemd/system/zookeeper.service <<'UNIT'
[Unit]
Description=ZooKeeper
After=network.target
[Service]
Type=simple
Environment=JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ExecStart=/opt/zookeeper/bin/zkServer.sh start-foreground
Restart=on-failure
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now zookeeper
touch /var/lib/bootstrap.done
EOF
}

ud_spark() {
  local nombre=$1 ip=${IP[$1]} mh="" extra=""
  if [[ $nombre == master-* ]]; then mh="export SPARK_MASTER_HOST=$ip"; fi
  if [[ $nombre == worker-1 ]]; then
    extra='curl -fsSL -o /tmp/awscli.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip && unzip -q /tmp/awscli.zip -d /tmp && /tmp/aws/install'
  fi
  ud_base
  cat <<EOF
pip3 install numpy==1.26.4 pandas==2.2.2
curl -fsSL --retry 5 -o /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-$SPARK_V/spark-$SPARK_V-bin-hadoop3.tgz
tar xzf /tmp/spark.tgz -C /opt
ln -sfn /opt/spark-$SPARK_V-bin-hadoop3 /opt/spark
curl -fsSL --retry 5 -o /opt/spark/jars/hadoop-aws-$HADOOP_AWS_V.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/$HADOOP_AWS_V/hadoop-aws-$HADOOP_AWS_V.jar
curl -fsSL --retry 5 -o /opt/spark/jars/aws-java-sdk-bundle-$AWS_SDK_V.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/$AWS_SDK_V/aws-java-sdk-bundle-$AWS_SDK_V.jar
cat > /opt/spark/conf/spark-env.sh <<'ENV'
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PYSPARK_PYTHON=python3
export SPARK_LOCAL_IP=$ip
$mh
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=4g
export SPARK_DAEMON_JAVA_OPTS="-Dspark.deploy.recoveryMode=ZOOKEEPER -Dspark.deploy.zookeeper.url=$ZKL -Dspark.deploy.zookeeper.dir=/spark"
ENV
cat > /opt/spark/conf/spark-defaults.conf <<'DEF'
spark.master                                    spark://$MASTERS
spark.driver.memory                             2g
spark.executor.memory                           3g
spark.executor.cores                            2
spark.sql.shuffle.partitions                    48
spark.hadoop.fs.s3a.impl                        org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider    com.amazonaws.auth.InstanceProfileCredentialsProvider
spark.hadoop.fs.s3a.endpoint                    s3.us-east-1.amazonaws.com
spark.hadoop.fs.s3a.connection.maximum          100
DEF
printf 'export SPARK_HOME=/opt/spark\nexport PATH=\$PATH:/opt/spark/bin:/opt/spark/sbin\n' > /etc/profile.d/spark.sh
chown -R ubuntu:ubuntu /opt/spark-$SPARK_V-bin-hadoop3
$extra
touch /var/lib/bootstrap.done
EOF
}

# ----------------------------- instancias ------------------------------------
ids_nodos() { # ids_nodos <estados> <nombre...>
  local est=$1; shift
  local nombres; nombres=$(IFS=,; echo "$*")
  aws ec2 describe-instances --filters "Name=tag:Name,Values=$nombres" "Name=tag:Proyecto,Values=u2-bigdata" \
    "Name=instance-state-name,Values=$est" --query 'Reservations[].Instances[].InstanceId' --output text
}

lanzar() {
  local n=$1 tipo=$2 disco=$3 ud=/tmp/ud-$1.sh
  if [[ -n "$(ids_nodos pending,running,stopping,stopped "$n")" ]]; then echo "   $n ya existe"; return; fi
  case $n in zk-*) ud_zk "${n#zk-}" > "$ud";; *) ud_spark "$n" > "$ud";; esac
  aws ec2 run-instances --image-id "$AMI" --instance-type "$tipo" --key-name "$KEY_NAME" \
    --iam-instance-profile Name="$PROFILE" --subnet-id "$SUBNET" --private-ip-address "${IP[$n]}" \
    --security-group-ids "$SG" --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=$disco,VolumeType=gp3}" \
    --user-data "file://$ud" --tag-specifications "$(tag instance "$n")" --query 'Instances[0].InstanceId' --output text \
    | sed "s/^/   $n (${IP[$n]}) -> /"
}

crear_nodos() {
  echo ">> AMI Ubuntu 22.04"
  AMI=$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id \
        --query Parameter.Value --output text 2>/dev/null || true)
  if nada "$AMI"; then
    AMI=$(aws ec2 describe-images --owners 099720109477 --filters 'Name=name,Values=ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*' \
          'Name=state,Values=available' --query 'sort_by(Images,&CreationDate)[-1].ImageId' --output text)
  fi
  echo "   AMI=$AMI"
  echo ">> Lanzando nodos (orden ZooKeeper -> masters -> workers)"
  for n in zk-1 zk-2 zk-3; do lanzar "$n" "$TIPO_ZK" 10; done
  for n in master-1 master-2; do lanzar "$n" "$TIPO_MASTER" 20; done
  lanzar worker-1 "$TIPO_WORKER" 40
  for n in worker-2 worker-3; do lanzar "$n" "$TIPO_WORKER" 20; done
}

arrancar_instancias() {
  echo ">> Arrancando instancias detenidas: ZooKeeper -> masters -> workers"
  local grupo ids
  for grupo in "zk-1 zk-2 zk-3" "master-1 master-2" "worker-1 worker-2 worker-3"; do
    # shellcheck disable=SC2086
    ids=$(ids_nodos stopped $grupo)
    if [[ -n "$ids" ]]; then
      # shellcheck disable=SC2086
      aws ec2 start-instances --instance-ids $ids >/dev/null
    fi
    # shellcheck disable=SC2086
    ids=$(ids_nodos pending,running $grupo)
    # shellcheck disable=SC2086
    if [[ -n "$ids" ]]; then aws ec2 wait instance-running --instance-ids $ids; fi
  done
}

cargar_ips() {
  PUB=()
  local n ip
  while read -r n ip; do PUB[$n]=$ip; done < <(aws ec2 describe-instances \
    --filters "Name=tag:Proyecto,Values=u2-bigdata" "Name=instance-state-name,Values=running" \
    --query 'Reservations[].Instances[].[Tags[?Key==`Name`]|[0].Value,PublicIpAddress]' --output text)
  for n in "${ORDEN[@]}"; do
    if [[ -z "${PUB[$n]:-}" ]]; then die "El nodo $n no está running. Ejecuta: $0 arrancar"; fi
  done
}

esperar_bootstrap() {
  local n t=0
  echo ">> Esperando instalación (user-data) en los 8 nodos (5-10 min la primera vez)"
  for n in "${ORDEN[@]}"; do
    until rssh "$n" 'test -f /var/lib/bootstrap.done' 2>/dev/null; do
      t=$((t+1))
      if (( t > 120 )); then die "bootstrap de $n no terminó. Revisa: ssh -i $KEY_FILE ubuntu@${PUB[$n]} 'tail -50 /var/log/bootstrap.log'"; fi
      sleep 10
    done
    echo "   $n listo"
  done
}

servicios() {
  echo ">> Servicios: ZooKeeper"
  local n i
  for n in zk-1 zk-2 zk-3; do rssh "$n" 'sudo systemctl start zookeeper'; done
  for n in zk-1 zk-2 zk-3; do
    for i in $(seq 1 40); do
      if rssh "$n" '/opt/zookeeper/bin/zkServer.sh status 2>&1' | grep -q Mode; then break; fi
      sleep 3
    done
  done
  echo ">> Servicios: masters (master-1 primero -> ALIVE; master-2 -> STANDBY)"
  for n in master-1 master-2; do
    rssh "$n" "$PUBDNS pgrep -f '[d]eploy.master.Master' >/dev/null || /opt/spark/sbin/start-master.sh" >/dev/null
    sleep 8
  done
  echo ">> Servicios: workers"
  for n in worker-1 worker-2 worker-3; do
    rssh "$n" "$PUBDNS pgrep -f '[d]eploy.worker.Worker' >/dev/null || /opt/spark/sbin/start-worker.sh spark://$MASTERS" >/dev/null
  done
  sleep 10
}

verificar() {
  cargar_ips
  echo "=== ZooKeeper (E9) ==="
  local n
  for n in zk-1 zk-2 zk-3; do
    printf '%s (%s): ' "$n" "${IP[$n]}"; date '+%F %T'
    rssh "$n" '/opt/zookeeper/bin/zkServer.sh status 2>&1' | grep Mode || echo "SIN RESPUESTA"
  done
  echo "=== Masters (E1/E2) ==="
  for n in master-1 master-2; do
    printf '%s (%s): ' "$n" "${IP[$n]}"
    rssh "$n" "curl -s ${IP[$n]}:8080/json/" | python3 -c '
import sys, json
try:
    d = json.load(sys.stdin)
    w = [x for x in d["workers"] if x["state"] == "ALIVE"]
    print(d["status"], "|", len(w), "workers ALIVE:", sorted(x["host"] for x in w))
except Exception:
    print("sin respuesta (master no iniciado?)")'
  done
  if [[ -n "$MI_IP" ]]; then
    echo "WebUI (desde tu navegador): http://${PUB[master-1]}:8080  y  http://${PUB[master-2]}:8080"
  fi
}

# ----------------------------- datos y prueba --------------------------------
datos() {
  cargar_ips
  local gen=${GENERADOR:-./generador_dataset.py}
  [[ -f "$gen" ]] || die "No encuentro $gen (ver README: extraer del notebook, o export GENERADOR=ruta)"
  bucket
  echo ">> Copiando generador a worker-1 y generando dataset (seed=$SEMILLA, gb=$GB)"
  scp ${SSH_OPTS/-n /} "$gen" "ubuntu@${PUB[worker-1]}:~/generador_dataset.py"
  rssh worker-1 "cd ~ && python3 generador_dataset.py --seed $SEMILLA --gb $GB --out ./dataset"
  echo ">> Subiendo a s3://$BUCKET/raw/<tabla>/<tabla>.csv"
  rssh worker-1 "export AWS_DEFAULT_REGION=us-east-1; for t in clientes productos ventas detalle_ventas navegacion_web; do /usr/local/bin/aws s3 cp ~/dataset/\$t.csv s3://$BUCKET/raw/\$t/\$t.csv; done; /usr/local/bin/aws s3 cp ~/dataset/_manifiesto.json s3://$BUCKET/results/_manifiesto.json"
  scp ${SSH_OPTS/-n /} "ubuntu@${PUB[worker-1]}:~/dataset/_manifiesto.json" ./_manifiesto.json
  echo ">> Listo. _manifiesto.json copiado a $(pwd) (E11: súbelo al repo; NO los CSV)."
  aws s3 ls "s3://$BUCKET/raw/" --recursive --human-readable --summarize
}

prueba() {
  cargar_ips
  local f=${SMOKE:-../src/00_smoke_test.py}
  [[ -f "$f" ]] || f=./src/00_smoke_test.py
  [[ -f "$f" ]] || die "No encuentro 00_smoke_test.py (SMOKE=ruta)"
  scp ${SSH_OPTS/-n /} "$f" "ubuntu@${PUB[worker-1]}:~/00_smoke_test.py"
  echo ">> spark-submit desde worker-1 (driver fuera de master-1)"
  rssh worker-1 "/opt/spark/bin/spark-submit --master spark://$MASTERS --name u2_smoke_test ~/00_smoke_test.py $BUCKET"
}

estado() {
  aws ec2 describe-instances --filters "Name=tag:Proyecto,Values=u2-bigdata" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query 'Reservations[].Instances[].[Tags[?Key==`Name`]|[0].Value,InstanceType,State.Name,PrivateIpAddress,PublicIpAddress]' --output text | sort | column -t
}

detener() {
  # shellcheck disable=SC2046
  local ids; ids=$(ids_nodos pending,running "${ORDEN[@]}")
  if [[ -z "$ids" ]]; then echo "Nada que detener"; return; fi
  # shellcheck disable=SC2086
  aws ec2 stop-instances --instance-ids $ids --query 'StoppingInstances[].[InstanceId,CurrentState.Name]' --output text
  echo "Detenidas. Para volver: $0 arrancar"
}

destruir() {
  read -rp "Esto ELIMINA instancias y red (el bucket y sus datos NO). Escribe SI: " r
  [[ "$r" == "SI" ]] || die "cancelado"
  local ids vpc
  ids=$(ids_nodos pending,running,stopping,stopped "${ORDEN[@]}")
  if [[ -n "$ids" ]]; then
    # shellcheck disable=SC2086
    aws ec2 terminate-instances --instance-ids $ids >/dev/null
    # shellcheck disable=SC2086
    aws ec2 wait instance-terminated --instance-ids $ids
  fi
  vpc=$(aws ec2 describe-vpcs --filters Name=tag:Name,Values=u2-vpc --query 'Vpcs[0].VpcId' --output text)
  if nada "$vpc"; then echo "No hay VPC u2-vpc"; return; fi
  local x
  for x in $(aws ec2 describe-security-groups --filters Name=vpc-id,Values="$vpc" Name=group-name,Values=u2-sg --query 'SecurityGroups[].GroupId' --output text); do
    aws ec2 delete-security-group --group-id "$x"
  done
  for x in $(aws ec2 describe-subnets --filters Name=vpc-id,Values="$vpc" --query 'Subnets[].SubnetId' --output text); do
    aws ec2 delete-subnet --subnet-id "$x"
  done
  for x in $(aws ec2 describe-route-tables --filters Name=vpc-id,Values="$vpc" Name=tag:Name,Values=u2-rt --query 'RouteTables[].RouteTableId' --output text); do
    aws ec2 delete-route-table --route-table-id "$x"
  done
  for x in $(aws ec2 describe-internet-gateways --filters Name=attachment.vpc-id,Values="$vpc" --query 'InternetGateways[].InternetGatewayId' --output text); do
    aws ec2 detach-internet-gateway --internet-gateway-id "$x" --vpc-id "$vpc"
    aws ec2 delete-internet-gateway --internet-gateway-id "$x"
  done
  aws ec2 delete-vpc --vpc-id "$vpc"
  echo "Eliminado."
}

# ----------------------------- main ------------------------------------------
cmd=${1:-crear}
case $cmd in
  crear)
    llave; red; bucket; crear_nodos
    arrancar_instancias; cargar_ips; esperar_bootstrap; servicios; verificar ;;
  arrancar)
    llave; VPC=$(aws ec2 describe-vpcs --filters Name=tag:Name,Values=u2-vpc --query 'Vpcs[0].VpcId' --output text)
    nada "$VPC" && die "No hay VPC: usa '$0 crear'"
    SG=$(aws ec2 describe-security-groups --filters Name=vpc-id,Values="$VPC" Name=group-name,Values=u2-sg --query 'SecurityGroups[0].GroupId' --output text)
    refrescar_sg; arrancar_instancias; cargar_ips; esperar_bootstrap; servicios; verificar ;;
  verificar) verificar ;;
  datos)     datos ;;
  prueba)    prueba ;;
  estado)    estado ;;
  detener)   detener ;;
  destruir)  destruir ;;
  *) die "comando desconocido: $cmd (crear|arrancar|verificar|datos|prueba|estado|detener|destruir)" ;;
esac
