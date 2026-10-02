#!/bin/sh
set -eu
KAFKA_BIN=/opt/kafka/bin
BROKERS=kafka:19092,kafka2:19092
# Migra tópicos já existentes sem apagar logs, offsets ou dados.
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BROKERS" --describe > /tmp/topics.txt
awk '
BEGIN {printf "{\"version\":1,\"partitions\":["; first=1}
/Partition: / {
  for(i=1;i<=NF;i++) {
    if($i=="Topic:") topic=$(i+1)
    if($i=="Partition:") partition=$(i+1)
    if($i=="Leader:") leader=$(i+1)
    if($i=="Replicas:") replicas=$(i+1)
  }
  count=split(replicas,ids,",")
  if (count==2 && ((ids[1]==1 && ids[2]==2) || (ids[1]==2 && ids[2]==1))) next
  if (!first) printf ","; first=0; changed++
  if (leader != 2) leader=1
  other=(leader==1 ? 2 : 1)
  printf "{\"topic\":\"%s\",\"partition\":%d,\"replicas\":[%d,%d]}",topic,partition,leader,other
}
END {print "]}"; print changed+0 > "/tmp/reassignment.count"}
' /tmp/topics.txt > /tmp/reassignment.json
if [ "$(cat /tmp/reassignment.count)" = "0" ]; then
  echo "Todos os tópicos já têm duas réplicas."
  exit 0
fi
"$KAFKA_BIN/kafka-reassign-partitions.sh" --bootstrap-server "$BROKERS" --reassignment-json-file /tmp/reassignment.json --execute
for attempt in $(seq 1 60); do
  "$KAFKA_BIN/kafka-reassign-partitions.sh" --bootstrap-server "$BROKERS" --reassignment-json-file /tmp/reassignment.json --verify > /tmp/verify.txt
  if ! awk '/is still in progress|There is an active reassignment|failed/ {found=1} END {exit !found}' /tmp/verify.txt; then
    cat /tmp/verify.txt
    exit 0
  fi
  sleep 2
done
cat /tmp/verify.txt
exit 1
