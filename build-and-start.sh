#!/usr/bin/env bash
set -euo pipefail

MASTER_CONTAINER="patroni-primary"
REPLICA_CONTAINER="patroni-replica"
sleep_s=10

master_is_up() {
  docker exec "$MASTER_CONTAINER" pg_isready -U postgres >/dev/null 2>&1
}

replica_is_up() {
  local running
  running="$(docker inspect -f '{{.State.Running}}' "$REPLICA_CONTAINER" 2>/dev/null | tr -d '\r' || true)"
  [[ "$running" == "true" ]] || return 1

  docker exec "$REPLICA_CONTAINER" patronictl list >/dev/null 2>&1
}

replica_status() {
  docker exec "$REPLICA_CONTAINER" patronictl list 2>/dev/null \
    | tr -d '\r' \
    | awk -F'|' '
        function trim(s){gsub(/^[ \t]+|[ \t]+$/, "", s); return s}
        $0 ~ /\|/ {
          role=trim($4); state=trim($5); lag=trim($7);
          if (role=="Replica") { print state "|" lag; exit 0 }
        }
        END { exit 1 }
      '
}

replica_is_synced() {
  local stlag state lag
  stlag="$(replica_status || true)"
  [[ -n "$stlag" ]] || return 1

  state="${stlag%%|*}"
  lag="${stlag#*|}"

  [[ "$state" == "streaming" || "$state" == "running" ]] || return 1
  [[ "$lag" == "" || "$lag" =~ ^0([.][0]+)?$ ]] || return 1
  return 0
}

echo "Docker-compose up"
docker-compose up -d

echo "1) Waiting for the master to start..."
until master_is_up; do sleep "$sleep_s"; done

echo "2) Waiting for the replica to start..."
until replica_is_up; do sleep "$sleep_s"; done

echo "3) Waiting for the replica to synchronize..."
while true; do
  stlag="$(replica_status || true)"
  if [[ -n "$stlag" ]]; then
    echo "   Replica status: ${stlag%%|*}, lag: ${stlag#*|}"
  else
    echo "   Replica status: (cannot read patronictl list yet)"
  fi

  replica_is_synced && break
  sleep "$sleep_s"
done

echo "4) Replica is online"
docker exec "${REPLICA_CONTAINER}" patronictl list