#!/usr/bin/env bash
# E2E на настоящем Spark (kind + Kubeflow Spark Operator): `Read → Filter →
# Aggregate → Publish Dataset`, Attempt Fence и ретрай пропавшей попытки.
#
# Поднимает kind с локальным OCI registry и Spark Operator, Control Plane через
# docker compose, собирает и пушит образы блоков, регистрирует их digest через
# `e3-register-block`, запускает `e3-execution-controller` и гоняет tests/e2e.
# В конце всё разбирает; E3_E2E_KEEP=1 оставляет стенд для отладки.
#
# Нужны Docker, helm и uv. kind (и kubectl, если его нет) скачиваются в .cache/bin.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
CACHE="$ROOT/.cache"
WORK="$CACHE/e2e"

KIND_VERSION=v0.33.0
KUBECTL_VERSION=v1.37.1
SPARK_OPERATOR_VERSION=2.5.2
CLUSTER=e3-e2e
REGISTRY=e3-kind-registry
REGISTRY_PORT=5001 # 5000 на macOS занят AirPlay
BLOCKS=(read filter aggregate)
PLATFORM_TOKEN=dev-platform-token # E3_PLATFORM_TOKEN сервиса api в docker-compose.yml

export KUBECONFIG="$WORK/kubeconfig" # не трогаем ~/.kube/config
export COMPOSE_PROJECT_NAME=e3-e2e
# Удалённую посреди запуска попытку reconciliation должна найти за секунды, а не минуты.
export E3_RECONCILE_AFTER_SECONDS=15
export PATH="$CACHE/bin:$PATH"

log() { printf '\n==> %s\n' "$*"; }

platform() {
  local os arch
  os=$(uname -s | tr '[:upper:]' '[:lower:]')
  arch=$(uname -m)
  case "$arch" in x86_64) arch=amd64 ;; aarch64 | arm64) arch=arm64 ;; esac
  echo "$os $arch"
}

install_tools() {
  mkdir -p "$CACHE/bin" "$WORK"
  read -r os arch < <(platform)
  if [[ "$(kind version 2>/dev/null)" != "kind $KIND_VERSION "* ]]; then
    log "kind $KIND_VERSION → .cache/bin"
    local url="https://github.com/kubernetes-sigs/kind/releases/download/$KIND_VERSION/kind-$os-$arch"
    curl -fsSL -o "$CACHE/bin/kind" "$url"
    echo "$(curl -fsSL "$url.sha256sum" | cut -d' ' -f1)  $CACHE/bin/kind" | shasum -a 256 -c -
    chmod +x "$CACHE/bin/kind"
  fi
  if ! command -v kubectl >/dev/null; then
    log "kubectl $KUBECTL_VERSION → .cache/bin"
    local url="https://dl.k8s.io/release/$KUBECTL_VERSION/bin/$os/$arch/kubectl"
    curl -fsSL -o "$CACHE/bin/kubectl" "$url"
    echo "$(curl -fsSL "$url.sha256")  $CACHE/bin/kubectl" | shasum -a 256 -c -
    chmod +x "$CACHE/bin/kubectl"
  fi
}

compose() {
  docker compose -f "$ROOT/docker-compose.yml" "$@"
}

teardown() {
  local status=$?
  set +e # разбираем до конца, даже если что-то уже остановлено
  if [[ -n "${CONTROLLER_PID:-}" ]]; then
    kill "$CONTROLLER_PID" 2>/dev/null
    wait "$CONTROLLER_PID" 2>/dev/null
  fi
  if [[ $status -ne 0 ]]; then
    log "упало: логи controller и SparkApplication"
    tail -n 50 "$WORK/controller.log" 2>/dev/null
    kubectl get sparkapplications -o wide 2>/dev/null
    kubectl logs -l spark-role=driver --tail=50 2>/dev/null
    kubectl get pods -A -o wide 2>/dev/null
    kubectl get events -A --sort-by=.lastTimestamp 2>/dev/null | tail -50
    compose logs --tail=50 api 2>/dev/null
  fi
  if [[ "${E3_E2E_KEEP:-}" == 1 ]]; then
    log "E3_E2E_KEEP=1: стенд оставлен (KUBECONFIG=$KUBECONFIG)"
    exit $status
  fi
  log "разбираем стенд"
  compose down -v --remove-orphans >/dev/null 2>&1
  kind delete cluster --name "$CLUSTER" >/dev/null 2>&1
  docker rm -f "$REGISTRY" >/dev/null 2>&1
  exit $status
}

start_registry() {
  log "локальный registry на localhost:$REGISTRY_PORT"
  if [[ "$(docker inspect -f '{{.State.Running}}' "$REGISTRY" 2>/dev/null)" != true ]]; then
    docker run -d --restart=always -p "127.0.0.1:$REGISTRY_PORT:5000" --network bridge \
      --name "$REGISTRY" registry:3 >/dev/null
  fi
}

create_cluster() {
  log "kind-кластер $CLUSTER"
  if ! kind get clusters | grep -qx "$CLUSTER"; then
    kind create cluster --name "$CLUSTER" --config "$ROOT/tests/e2e/kind.yaml" --wait 120s
  fi
  kind get kubeconfig --name "$CLUSTER" >"$KUBECONFIG"
  # Ноды тянут localhost:5001/... из registry по сети kind.
  for node in $(kind get nodes --name "$CLUSTER"); do
    docker exec "$node" mkdir -p "/etc/containerd/certs.d/localhost:$REGISTRY_PORT"
    printf '[host."http://%s:5000"]\n' "$REGISTRY" |
      docker exec -i "$node" cp /dev/stdin "/etc/containerd/certs.d/localhost:$REGISTRY_PORT/hosts.toml"
  done
  connect_to_kind "$REGISTRY"
}

connect_to_kind() {
  if [[ "$(docker inspect -f '{{json .NetworkSettings.Networks.kind}}' "$1")" == null ]]; then
    docker network connect kind "$1"
  fi
}

install_spark_operator() {
  log "Kubeflow Spark Operator $SPARK_OPERATOR_VERSION"
  helm upgrade --install spark-operator spark-operator \
    --repo https://kubeflow.github.io/spark-operator --version "$SPARK_OPERATOR_VERSION" \
    --namespace spark-operator --create-namespace \
    --set 'spark.jobNamespaces={default}' --wait --timeout 5m
}

start_control_plane() {
  log "Control Plane: PostgreSQL, Kafka, MinIO, API"
  compose up -d --build
  until curl -fsS http://localhost:8000/docs >/dev/null 2>&1; do sleep 1; done
  # Топики создаёт Run Orchestrator при старте API; controller слушает уже готовые.
  until compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --list | grep -qx e3.execution.commands.v1; do sleep 1; done
  local minio
  minio=$(compose ps -q minio)
  connect_to_kind "$minio"
  MINIO_IN_KIND=$(docker inspect -f '{{.NetworkSettings.Networks.kind.IPAddress}}' "$minio")
  kubectl create secret generic e3-s3 \
    --from-literal=AWS_ACCESS_KEY_ID=minioadmin --from-literal=AWS_SECRET_ACCESS_KEY=minioadmin \
    --dry-run=client -o yaml | kubectl apply -f -
}

register_block() {
  local block=$1 image="localhost:$REGISTRY_PORT/e3-$1:e2e" digest
  log "образ $block → $image"
  docker build -q -f "$ROOT/blocks/Dockerfile" --build-arg "BLOCK=$block" -t "$image" "$ROOT" >/dev/null
  docker push -q "$image" >/dev/null
  digest=$(docker inspect -f '{{index .RepoDigests 0}}' "$image")
  sed "s|@EXECUTABLE_REF@|$digest|" "$ROOT/blocks/$block/block.yaml" >"$WORK/$block.yaml"
  E3_API_URL=http://localhost:8000 E3_PLATFORM_TOKEN=$PLATFORM_TOKEN \
    uv run --project "$ROOT" e3-register-block "$WORK/$block.yaml"
}

controller_env() {
  export E3_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
  export E3_S3_ENDPOINT_URL=http://localhost:9000
  export E3_SPARK_S3_ENDPOINT_URL="http://$MINIO_IN_KIND:9000"
  # На 2 vCPU раннера CI целое ядро на driver и executor не помещается рядом с системой kind.
  export E3_SPARK_CORE_REQUEST=250m
}

start_controller() {
  log "e3-execution-controller (лог: $WORK/controller.log)"
  uv run --project "$ROOT" e3-execution-controller >"$WORK/controller.log" 2>&1 &
  CONTROLLER_PID=$!
}

main() {
  trap teardown EXIT
  install_tools
  start_registry
  create_cluster
  install_spark_operator
  start_control_plane
  for block in "${BLOCKS[@]}"; do register_block "$block"; done
  controller_env
  start_controller
  log "E2E: Read → Filter → Aggregate → Publish Dataset, Attempt Fence"
  E3_E2E_API_URL=http://localhost:8000 uv run --project "$ROOT" pytest "$ROOT/tests/e2e" -v
}

main "$@"
