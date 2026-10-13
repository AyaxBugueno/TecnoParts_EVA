#!/bin/bash
# =============================================================================
#  reconstruir_cluster.sh  ·  Clúster Spark HA + ZooKeeper en AWS Academy
#  Ejecutar desde AWS CloudShell (us-east-1).  Ver README.md.
#
#  Uso:  ./reconstruir_cluster.sh <comando>
#    desplegar   Crea lo que falte y arranca lo que esté detenido
#                (cubre escenario (a) recursos eliminados y (b) instancias
#                detenidas). Orden: ZooKeeper -> masters -> workers.
#    verificar   zkServer.sh status x3, WebUI :8080 de ambos masters, workers.
#    estado      Tabla de instancias (estado, IP privada, IP pública).
#    detener     Detiene las 8 instancias (workers -> masters -> ZooKeeper).
#    borrar      Elimina TODO el laboratorio (pide confirmación; --yes la omite).
#    webui IP    Abre 8080/8081/4040 SOLO para IP/32 (tu IP de navegador).
#    userdata N  Imprime el user data que recibiría el nodo N (sin usar AWS).
#
#  Sin access keys ni .pem en el código: la llave se genera en ~/.ssh y el acceso
#  a S3 se hace con LabInstanceProfile.
# =============================================================================
set -euo pipefail
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""

# ---------------------------- Parámetros editables ---------------------------
PROJ="${PROJ:-spark-ha}"
KEY="spark-key"
PROFILE="LabInstanceProfile"
AZ="us-east-1a"
VPC_CIDR="10.0.0.0/16"; SUBNET_CIDR="10.0.1.0/24"
SPARK_VERSION="${SPARK_VERSION:-3.5.0}"     # enunciado: Spark 3.5.0 / Hadoop 3
ZK_VERSION="${ZK_VERSION:-3.9.5}"           # ver nota en README (si no existe, usar 3.8.6)
SPARK_TYPE="${SPARK_TYPE:-t3.medium}"       # masters y workers (2 vCPU, 4 GiB)
ZK_TYPE="${ZK_TYPE:-t3.small}"              # ZooKeeper es liviano
DISK_GB=20

# nombre:ip_privada:rol:id_zk   (el orden es el orden de arranque)
NODES=(
  "zk-1:10.0.1.20:zk:1"
  "zk-2:10.0.1.21:zk:2"
  "zk-3:10.0.1.22:zk:3"
  "master-1:10.0.1.10:master:0"
  "master-2:10.0.1.14:master:0"
  "worker-1:10.0.1.11:worker:0"
  "worker-2:10.0.1.12:worker:0"
  "worker-3:10.0.1.13:worker:0"
)
ZK_CONN="10.0.1.20:2181,10.0.1.21:2181,10.0.1.22:2181"
MASTER_URLS="spark://10.0.1.10:7077,10.0.1.14:7077"
JH="/usr/lib/jvm/java-17-openjdk-amd64"
ZKBIN="JAVA_HOME=$JH /opt/zookeeper/bin/zkServer.sh"
SSH_OPTS=(-n -o BatchMode=yes -o ConnectTimeout=8)

# --------------------------------- Utilidades --------------------------------
paso(){ echo; echo "=== $*"; }
info(){ echo "  $*"; }
die(){ echo "ERROR: $*" >&2; exit 1; }
node_field(){ local IFS=:; local a; read -ra a <<<"$1"; case $2 in name) echo "${a[0]}";; ip) echo "${a[1]}";; role) echo "${a[2]}";; zid) echo "${a[3]}";; esac; }
nodes_of(){ local n; for n in "${NODES[@]}"; do [ "$(node_field "$n" role)" = "$1" ] && echo "$n"; done; return 0; }
all_names(){ local n; for n in "${NODES[@]}"; do node_field "$n" name; done; }
itype(){ [ "$1" = zk ] && echo "$ZK_TYPE" || echo "$SPARK_TYPE"; }

FILT_LIVE="Name=instance-state-name,Values=pending,running,stopping,stopped"
inst_id(){    aws ec2 describe-instances --filters "Name=tag:Project,Values=$PROJ" "Name=tag:Name,Values=$1" "$FILT_LIVE" --query "Reservations[].Instances[].InstanceId" --output text; }
inst_state(){ aws ec2 describe-instances --filters "Name=tag:Project,Values=$PROJ" "Name=tag:Name,Values=$1" "$FILT_LIVE" --query "Reservations[].Instances[].State.Name" --output text; }
inst_pub(){   local p; p=$(aws ec2 describe-instances --filters "Name=tag:Project,Values=$PROJ" "Name=tag:Name,Values=$1" "Name=instance-state-name,Values=running" --query "Reservations[].Instances[].PublicIpAddress" --output text); [ "$p" = "None" ] && p=""; echo "$p"; }
none_to_empty(){ local v; read -r v || true; [ "$v" = "None" ] && v=""; echo "$v"; }
rssh(){ local h=$1; shift; ssh "${SSH_OPTS[@]}" "$h" "$@"; }

# ------------------------------ User data (bootstrap) ------------------------
# Cabecera generada (valores de este nodo) + cuerpo estático. Se ejecuta como root
# en el primer arranque (cloud-init). Deja ~/BOOTSTRAP_OK al terminar.
gen_userdata(){
  local name=$1 ip=$2 role=$3 zid=$4 n
  {
    echo '#!/bin/bash'
    echo 'set -euxo pipefail'
    echo "NODE_NAME=$name; NODE_IP=$ip; ROLE=$role; ZK_ID=$zid"
    echo "ZK_VERSION=$ZK_VERSION; SPARK_VERSION=$SPARK_VERSION"
    echo "ZK_CONN=$ZK_CONN; MASTER_URLS=$MASTER_URLS"
    echo "cat >> /etc/hosts <<'H'"
    for n in "${NODES[@]}"; do echo "$(node_field "$n" ip) $(node_field "$n" name)"; done
    echo "H"
    echo "cat > /tmp/zk-servers.txt <<'Z'"
    for n in $(nodes_of zk); do echo "server.$(node_field "$n" zid)=$(node_field "$n" ip):2888:3888"; done
    echo "Z"
    cat <<'BODY'
JH=/usr/lib/jvm/java-17-openjdk-amd64
hostnamectl set-hostname "$NODE_NAME"
echo "preserve_hostname: true" > /etc/cloud/cloud.cfg.d/99-hostname.cfg

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y openjdk-17-jre-headless python3 python3-pip python3-numpy curl

fetch(){ # fetch <url_principal> <url_archivo> <destino>
  curl -fSL --retry 3 -o "$3" "$1" || curl -fSL --retry 3 -o "$3" "$2"
}

cat > /etc/profile.d/bigdata.sh <<'P'
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_HOME=/opt/spark
export ZOOKEEPER_HOME=/opt/zookeeper
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin:$ZOOKEEPER_HOME/bin
export PYSPARK_PYTHON=python3
P

cd /opt
if [ "$ROLE" = "zk" ]; then
  # ---------------- ZooKeeper: zoo.cfg + myid ----------------
  ZPKG="apache-zookeeper-${ZK_VERSION}-bin"
  fetch "https://dlcdn.apache.org/zookeeper/zookeeper-${ZK_VERSION}/${ZPKG}.tar.gz" \
        "https://archive.apache.org/dist/zookeeper/zookeeper-${ZK_VERSION}/${ZPKG}.tar.gz" "${ZPKG}.tar.gz"
  tar -xzf "${ZPKG}.tar.gz" && rm "${ZPKG}.tar.gz"
  ln -s "/opt/${ZPKG}" /opt/zookeeper
  mkdir -p /var/lib/zookeeper
  echo "$ZK_ID" > /var/lib/zookeeper/myid
  cat > /opt/zookeeper/conf/zoo.cfg <<'Z'
tickTime=2000
initLimit=10
syncLimit=5
dataDir=/var/lib/zookeeper
clientPort=2181
admin.enableServer=false
Z
  cat /tmp/zk-servers.txt >> /opt/zookeeper/conf/zoo.cfg
  echo "JAVA_HOME=$JH" > /opt/zookeeper/conf/java.env
  chown -R ubuntu:ubuntu "/opt/${ZPKG}" /var/lib/zookeeper
else
  # ---------------- Spark (master o worker) en modo HA ----------------
  PKG="spark-${SPARK_VERSION}-bin-hadoop3"
  fetch "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${PKG}.tgz" \
        "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${PKG}.tgz" "${PKG}.tgz"
  tar -xzf "${PKG}.tgz" && rm "${PKG}.tgz"
  ln -s "/opt/${PKG}" /opt/spark
  chown -R ubuntu:ubuntu "/opt/${PKG}"
  cat > /opt/spark/conf/spark-env.sh <<S
export JAVA_HOME=$JH
export SPARK_MASTER_HOST=${NODE_IP}
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export SPARK_LOCAL_IP=${NODE_IP}
export SPARK_WORKER_PORT=7000
export SPARK_WORKER_WEBUI_PORT=8081
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=3g
export PYSPARK_PYTHON=python3
export SPARK_DAEMON_JAVA_OPTS="-Dspark.deploy.recoveryMode=ZOOKEEPER -Dspark.deploy.zookeeper.url=${ZK_CONN} -Dspark.deploy.zookeeper.dir=/spark"
S
  cat > /opt/spark/conf/spark-defaults.conf <<D
spark.master            ${MASTER_URLS}
spark.driver.port       7001
spark.blockManager.port 7002
D
  chown ubuntu:ubuntu /opt/spark/conf/spark-env.sh /opt/spark/conf/spark-defaults.conf
fi
touch /home/ubuntu/BOOTSTRAP_OK
BODY
  }
}

# ---------------------------------- Red / SG ---------------------------------
preflight(){
  command -v aws >/dev/null || die "No se encontró aws CLI (use CloudShell)."
  aws sts get-caller-identity --query Account --output text >/dev/null 2>&1 \
    || die "Credenciales inválidas o sesión del lab vencida (ExpiredToken). Reinicie el lab."
  aws iam get-instance-profile --instance-profile-name "$PROFILE" >/dev/null 2>&1 \
    || echo "  AVISO: no pude verificar $PROFILE (puede ser solo falta de permiso IAM de lectura)."
}

ensure_network(){
  paso "Red (VPC, subred, IGW, rutas, Security Group)"
  VPC_ID=$(aws ec2 describe-vpcs --filters "Name=tag:Name,Values=$PROJ-vpc" --query "Vpcs[0].VpcId" --output text | none_to_empty)
  if [ -z "$VPC_ID" ]; then
    VPC_ID=$(aws ec2 create-vpc --cidr-block "$VPC_CIDR" --tag-specifications "ResourceType=vpc,Tags=[{Key=Name,Value=$PROJ-vpc}]" --query Vpc.VpcId --output text)
    aws ec2 modify-vpc-attribute --vpc-id "$VPC_ID" --enable-dns-hostnames '{"Value":true}'
    info "VPC creada: $VPC_ID"
  else info "VPC existente: $VPC_ID"; fi

  SUBNET_ID=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC_ID" --query "Subnets[0].SubnetId" --output text | none_to_empty)
  if [ -z "$SUBNET_ID" ]; then
    SUBNET_ID=$(aws ec2 create-subnet --vpc-id "$VPC_ID" --cidr-block "$SUBNET_CIDR" --availability-zone "$AZ" \
      --tag-specifications "ResourceType=subnet,Tags=[{Key=Name,Value=$PROJ-subnet}]" --query Subnet.SubnetId --output text)
    aws ec2 modify-subnet-attribute --subnet-id "$SUBNET_ID" --map-public-ip-on-launch
    info "Subred creada: $SUBNET_ID"
  else info "Subred existente: $SUBNET_ID"; fi

  IGW_ID=$(aws ec2 describe-internet-gateways --filters "Name=attachment.vpc-id,Values=$VPC_ID" --query "InternetGateways[0].InternetGatewayId" --output text | none_to_empty)
  if [ -z "$IGW_ID" ]; then
    IGW_ID=$(aws ec2 create-internet-gateway --tag-specifications "ResourceType=internet-gateway,Tags=[{Key=Name,Value=$PROJ-igw}]" --query InternetGateway.InternetGatewayId --output text)
    aws ec2 attach-internet-gateway --internet-gateway-id "$IGW_ID" --vpc-id "$VPC_ID"
    info "IGW creado: $IGW_ID"
  else info "IGW existente: $IGW_ID"; fi

  RT_ID=$(aws ec2 describe-route-tables --filters "Name=vpc-id,Values=$VPC_ID" "Name=tag:Name,Values=$PROJ-rt" --query "RouteTables[0].RouteTableId" --output text | none_to_empty)
  if [ -z "$RT_ID" ]; then
    RT_ID=$(aws ec2 create-route-table --vpc-id "$VPC_ID" --tag-specifications "ResourceType=route-table,Tags=[{Key=Name,Value=$PROJ-rt}]" --query RouteTable.RouteTableId --output text)
    aws ec2 create-route --route-table-id "$RT_ID" --destination-cidr-block 0.0.0.0/0 --gateway-id "$IGW_ID" >/dev/null
    aws ec2 associate-route-table --route-table-id "$RT_ID" --subnet-id "$SUBNET_ID" >/dev/null
    info "Tabla de rutas creada: $RT_ID"
  else info "Tabla de rutas existente: $RT_ID"; fi

  SG_ID=$(aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$VPC_ID" "Name=group-name,Values=$PROJ-sg" --query "SecurityGroups[0].GroupId" --output text | none_to_empty)
  if [ -z "$SG_ID" ]; then
    SG_ID=$(aws ec2 create-security-group --group-name "$PROJ-sg" --description "Spark HA + ZooKeeper" --vpc-id "$VPC_ID" --query GroupId --output text)
    aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --protocol -1 --source-group "$SG_ID" >/dev/null
    info "Security Group creado: $SG_ID (todo el tráfico entre miembros del SG)"
  else info "Security Group existente: $SG_ID"; fi
}

refresh_ssh_rule(){   # SSH (22) solo desde la IP actual de CloudShell; retira reglas viejas
  local my old c
  my=$(curl -s https://checkip.amazonaws.com | tr -d '[:space:]')
  [ -n "$my" ] || die "No pude obtener la IP pública de CloudShell."
  old=$(aws ec2 describe-security-groups --group-ids "$SG_ID" --query 'SecurityGroups[0].IpPermissions[?FromPort==`22`].IpRanges[].CidrIp' --output text)
  for c in $old; do
    if [ "$c" != "$my/32" ]; then aws ec2 revoke-security-group-ingress --group-id "$SG_ID" --protocol tcp --port 22 --cidr "$c" >/dev/null; fi
  done
  aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --protocol tcp --port 22 --cidr "$my/32" >/dev/null 2>&1 || true
  info "SSH (22) autorizado solo desde $my/32"
}

ensure_key(){
  paso "Llave SSH ($KEY)"
  mkdir -p ~/.ssh && chmod 700 ~/.ssh
  local in_aws=0
  aws ec2 describe-key-pairs --key-names "$KEY" >/dev/null 2>&1 && in_aws=1
  if [ $in_aws -eq 1 ] && [ -f ~/.ssh/$KEY.pem ]; then info "Llave existente (AWS + ~/.ssh/$KEY.pem)"
  elif [ $in_aws -eq 1 ]; then
    die "$KEY existe en AWS pero falta ~/.ssh/$KEY.pem (AWS no permite recuperarla). Ejecute './reconstruir_cluster.sh borrar' y luego 'desplegar'."
  else
    aws ec2 create-key-pair --key-name "$KEY" --key-type ed25519 --query KeyMaterial --output text > ~/.ssh/$KEY.pem
    chmod 400 ~/.ssh/$KEY.pem; info "Llave creada en ~/.ssh/$KEY.pem (NO la suba a Git)"
  fi
}

find_ami(){
  AMI_ID=$(aws ec2 describe-images --owners 099720109477 \
    --filters "Name=name,Values=ubuntu/images/hvm-ssd*/ubuntu-jammy-22.04-amd64-server-*" "Name=state,Values=available" \
    --query "sort_by(Images,&CreationDate)[-1].ImageId" --output text)
  info "AMI Ubuntu 22.04: $AMI_ID"
}

# ------------------------------ Instancias -----------------------------------
launch_node(){   # crea una instancia con IP privada fija, LabInstanceProfile y spark-key
  local name=$1 ip=$2 role=$3 zid=$4 ud id
  ud=$(mktemp /tmp/ud-"$name".XXXXXX); gen_userdata "$name" "$ip" "$role" "$zid" > "$ud"
  id=$(aws ec2 run-instances --image-id "$AMI_ID" --instance-type "$(itype "$role")" --key-name "$KEY" \
    --subnet-id "$SUBNET_ID" --private-ip-address "$ip" --security-group-ids "$SG_ID" \
    --iam-instance-profile "Name=$PROFILE" \
    --block-device-mappings "[{\"DeviceName\":\"/dev/sda1\",\"Ebs\":{\"VolumeSize\":$DISK_GB,\"VolumeType\":\"gp3\"}}]" \
    --user-data "file://$ud" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$name},{Key=Project,Value=$PROJ},{Key=Rol,Value=$role}]" \
    --query "Instances[0].InstanceId" --output text)
  rm -f "$ud"; info "creado   $name $ip $id"; CREATED_IDS+=("$id")
}

ensure_group(){  # ensure_group <rol>: crea faltantes, arranca detenidos (en orden de grupo)
  local role=$1 n name ip zid st id; local to_start=()
  paso "Instancias: grupo '$role'"
  for n in $(nodes_of "$role"); do
    name=$(node_field "$n" name); ip=$(node_field "$n" ip); zid=$(node_field "$n" zid)
    st=$(inst_state "$name")
    case "$st" in
      "")               launch_node "$name" "$ip" "$role" "$zid" ;;
      stopped)          to_start+=("$(inst_id "$name")"); info "detenida $name -> se arrancará" ;;
      stopping)         id=$(inst_id "$name"); aws ec2 wait instance-stopped --instance-ids "$id"; to_start+=("$id"); info "detenida $name -> se arrancará" ;;
      running|pending)  info "activa   $name ($st)" ;;
      *) die "Estado inesperado de $name: $st" ;;
    esac
  done
  if [ ${#to_start[@]} -gt 0 ]; then
    aws ec2 start-instances --instance-ids "${to_start[@]}" >/dev/null
    info "esperando chequeos de estado del grupo '$role'..."
    aws ec2 wait instance-status-ok --instance-ids "${to_start[@]}"
  fi
}

write_ssh_config(){   # IP públicas cambian al reiniciar: reescribe solo nuestro bloque
  local name ip; touch ~/.ssh/config
  sed -i '/^# BEGIN spark-ha$/,/^# END spark-ha$/d' ~/.ssh/config
  {
    echo "# BEGIN spark-ha"
    for name in $(all_names); do
      ip=$(inst_pub "$name"); [ -n "$ip" ] || continue
      cat <<CFG
Host $name
  HostName $ip
  User ubuntu
  IdentityFile ~/.ssh/$KEY.pem
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
  LogLevel ERROR
CFG
    done
    echo "# END spark-ha"
  } >> ~/.ssh/config
  chmod 600 ~/.ssh/config
}

wait_bootstrap(){
  paso "Esperando instalación en los nodos (BOOTSTRAP_OK)"
  local name i
  for name in $(all_names); do
    for i in $(seq 1 90); do
      if rssh "$name" 'test -f ~/BOOTSTRAP_OK' 2>/dev/null; then info "OK  $name"; continue 2; fi
      sleep 10
    done
    die "$name no terminó el bootstrap. Revise: ssh $name 'sudo tail -n 40 /var/log/cloud-init-output.log'"
  done
}

# ------------------------------- Servicios -----------------------------------
master_json(){ rssh "$1" 'curl -s localhost:8080/json/' 2>/dev/null | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("status","?"), d.get("aliveworkers",0))' 2>/dev/null || echo "? 0"; }

zk_summary(){   # imprime modo de cada ZK y deja LEADERS/FOLLOWERS
  LEADERS=0; FOLLOWERS=0; local n name m
  for n in $(nodes_of zk); do
    name=$(node_field "$n" name)
    m=$(rssh "$name" "$ZKBIN status 2>&1" 2>/dev/null | grep -o 'Mode: [a-z]*' || true)
    printf '  %-8s %s\n' "$name" "${m:-(sin respuesta)}"
    case "$m" in *leader) LEADERS=$((LEADERS+1));; *follower) FOLLOWERS=$((FOLLOWERS+1));; esac
  done
}

iniciar_servicios(){
  local n name i
  paso "Servicios 1/3: ZooKeeper"
  for n in $(nodes_of zk); do name=$(node_field "$n" name); rssh "$name" "$ZKBIN start" >/dev/null 2>&1 || true; info "start $name"; done
  for i in $(seq 1 20); do
    zk_summary >/dev/null; [ $LEADERS -eq 1 ] && [ $FOLLOWERS -eq 2 ] && break; sleep 3
  done
  zk_summary
  [ $LEADERS -eq 1 ] && [ $FOLLOWERS -eq 2 ] || die "El ensemble ZooKeeper no formó quórum (1 leader + 2 followers)."

  paso "Servicios 2/3: Masters (master-1 primero, luego master-2 queda STANDBY)"
  for n in $(nodes_of master); do
    name=$(node_field "$n" name)
    rssh "$name" "/opt/spark/sbin/start-master.sh" >/dev/null 2>&1 || true
    for i in $(seq 1 20); do
      read -r st _ <<<"$(master_json "$name")"
      case "$st" in ALIVE|STANDBY) break;; esac; sleep 3
    done
    info "$name -> ${st:-?}"
  done

  paso "Servicios 3/3: Workers (registro contra $MASTER_URLS)"
  for n in $(nodes_of worker); do
    name=$(node_field "$n" name)
    rssh "$name" "/opt/spark/sbin/start-worker.sh $MASTER_URLS" >/dev/null 2>&1 || true; info "start $name"
  done
  sleep 10
}

cmd_verificar(){
  local rc=0 n name st aw alive=0 standby=0 workers=0 ip
  paso "ZooKeeper (zkServer.sh status en zk-1, zk-2, zk-3)  [E9]"
  zk_summary
  if [ $LEADERS -eq 1 ] && [ $FOLLOWERS -eq 2 ]; then info "OK: 1 leader + 2 followers"; else info "FALLA: se esperaba 1 leader + 2 followers"; rc=1; fi

  paso "Spark Masters (WebUI :8080)  [E1/E2]"
  for n in $(nodes_of master); do
    name=$(node_field "$n" name); read -r st aw <<<"$(master_json "$name")"; ip=$(inst_pub "$name")
    printf '  %-9s %-9s workers vivos: %s   http://%s:8080\n' "$name" "$st" "$aw" "${ip:-?}"
    case "$st" in ALIVE) alive=$((alive+1)); workers=$aw;; STANDBY) standby=$((standby+1));; esac
  done
  if [ $alive -eq 1 ] && [ $standby -eq 1 ] && [ "$workers" -eq 3 ]; then info "OK: 1 ALIVE + 1 STANDBY + 3 workers"; else info "FALLA: se esperaba 1 ALIVE, 1 STANDBY y 3 workers"; rc=1; fi
  return $rc
}

cmd_desplegar(){
  preflight; ensure_network; ensure_key; refresh_ssh_rule; find_ami
  CREATED_IDS=()
  ensure_group zk; ensure_group master; ensure_group worker
  if [ ${#CREATED_IDS[@]} -gt 0 ]; then
    paso "Esperando chequeos de estado de las instancias nuevas (3-5 min)"
    aws ec2 wait instance-status-ok --instance-ids "${CREATED_IDS[@]}"
  fi
  write_ssh_config; wait_bootstrap; iniciar_servicios; cmd_verificar || true
  echo; echo "DESPLIEGUE LISTO. Para abrir WebUI en su navegador: ./reconstruir_cluster.sh webui <SU_IP_PUBLICA>"
}

cmd_estado(){
  aws ec2 describe-instances --filters "Name=tag:Project,Values=$PROJ" "$FILT_LIVE" \
    --query "Reservations[].Instances[].[Tags[?Key=='Name']|[0].Value,State.Name,PrivateIpAddress,PublicIpAddress,InstanceType]" --output table
}

cmd_detener(){
  local role ids n id
  for role in worker master zk; do
    ids=()
    for n in $(nodes_of "$role"); do
      if [ "$(inst_state "$(node_field "$n" name)")" = running ]; then ids+=("$(inst_id "$(node_field "$n" name)")"); fi
    done
    if [ ${#ids[@]} -gt 0 ]; then paso "Deteniendo grupo '$role'"; aws ec2 stop-instances --instance-ids "${ids[@]}" >/dev/null; aws ec2 wait instance-stopped --instance-ids "${ids[@]}"; fi
  done
  echo "Instancias detenidas. Para retomar: ./reconstruir_cluster.sh desplegar"
}

cmd_webui(){
  local cidr=${1:-}
  [ -n "$cidr" ] || die "Uso: webui <IP_PUBLICA_DE_SU_NAVEGADOR>  (abra https://checkip.amazonaws.com)"
  [[ "$cidr" == */* ]] || cidr="$cidr/32"
  [[ "$cidr" == */32 ]] || die "Solo se permite una IP (/32). No abra WebUI a 0.0.0.0/0."
  [ "$cidr" != "0.0.0.0/32" ] || die "IP inválida."
  SG_ID=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=$PROJ-sg" --query "SecurityGroups[0].GroupId" --output text | none_to_empty)
  [ -n "$SG_ID" ] || die "No existe el Security Group $PROJ-sg."
  local p
  for p in 8080-8081 4040; do
    aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --protocol tcp --port "$p" --cidr "$cidr" >/dev/null 2>&1 || true
  done
  echo "WebUI 8080-8081 y 4040 abiertas solo para $cidr"
}

cmd_borrar(){
  if [ "${1:-}" != "--yes" ]; then read -r -p "Esto ELIMINA instancias, VPC y llave del proyecto '$PROJ'. Escriba SI para continuar: " a; [ "$a" = "SI" ] || die "Cancelado."; fi
  preflight
  local ids vpc sg sn rt igw
  ids=$(aws ec2 describe-instances --filters "Name=tag:Project,Values=$PROJ" "$FILT_LIVE" --query "Reservations[].Instances[].InstanceId" --output text)
  if [ -n "$ids" ]; then paso "Terminando instancias"; aws ec2 terminate-instances --instance-ids $ids >/dev/null; aws ec2 wait instance-terminated --instance-ids $ids; fi
  for vpc in $(aws ec2 describe-vpcs --filters "Name=tag:Name,Values=$PROJ-vpc" --query "Vpcs[].VpcId" --output text); do
    paso "Limpiando $vpc"
    for sg in $(aws ec2 describe-security-groups --filters Name=vpc-id,Values="$vpc" --query "SecurityGroups[?GroupName!='default'].GroupId" --output text); do
      aws ec2 revoke-security-group-ingress --group-id "$sg" --protocol -1 --source-group "$sg" >/dev/null 2>&1 || true
      aws ec2 delete-security-group --group-id "$sg"
    done
    for sn in $(aws ec2 describe-subnets --filters Name=vpc-id,Values="$vpc" --query "Subnets[].SubnetId" --output text); do aws ec2 delete-subnet --subnet-id "$sn"; done
    for rt in $(aws ec2 describe-route-tables --filters Name=vpc-id,Values="$vpc" "Name=tag:Name,Values=$PROJ-rt" --query "RouteTables[].RouteTableId" --output text); do aws ec2 delete-route-table --route-table-id "$rt"; done
    for igw in $(aws ec2 describe-internet-gateways --filters Name=attachment.vpc-id,Values="$vpc" --query "InternetGateways[].InternetGatewayId" --output text); do
      aws ec2 detach-internet-gateway --internet-gateway-id "$igw" --vpc-id "$vpc"; aws ec2 delete-internet-gateway --internet-gateway-id "$igw"
    done
    aws ec2 delete-vpc --vpc-id "$vpc" && info "VPC $vpc eliminada"
  done
  aws ec2 delete-key-pair --key-name "$KEY" >/dev/null 2>&1 || true
  rm -f ~/.ssh/$KEY.pem
  [ -f ~/.ssh/config ] && sed -i '/^# BEGIN spark-ha$/,/^# END spark-ha$/d' ~/.ssh/config
  echo "LIMPIEZA TERMINADA"
}

cmd_userdata(){
  local want=${1:-}; [ -n "$want" ] || die "Uso: userdata <nodo>   (p. ej. zk-1, master-1, worker-2)"
  local n; for n in "${NODES[@]}"; do
    if [ "$(node_field "$n" name)" = "$want" ]; then gen_userdata "$want" "$(node_field "$n" ip)" "$(node_field "$n" role)" "$(node_field "$n" zid)"; return; fi
  done; die "Nodo desconocido: $want"
}

# -------------------------------- Dispatcher ---------------------------------
case "${1:-}" in
  desplegar) cmd_desplegar ;;
  verificar) cmd_verificar ;;
  estado)    cmd_estado ;;
  detener)   cmd_detener ;;
  borrar)    cmd_borrar "${2:-}" ;;
  webui)     cmd_webui "${2:-}" ;;
  userdata)  cmd_userdata "${2:-}" ;;
  *) sed -n '2,20p' "$0"; exit 1 ;;
esac
