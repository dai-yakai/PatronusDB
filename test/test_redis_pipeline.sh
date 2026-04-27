#!/bin/bash

# test redis pipeline and PDB batch process 
BENCH_BIN="/home/redis-8.0.0/src/redis-benchmark"
HOST_redis=$1
PORT_redis=$2
BATCH_NUM=$3
HOST_PDB=$4
PORT_PDB=$5

echo "redis echo test result:"
$BENCH_BIN -h $HOST_redis -p $PORT_redis -t PING_MBULK -n 1000000 -c 1 -P 5000 -q
echo "PDB echo test result:"
$BENCH_BIN -h $HOST_PDB -p $PORT_PDB -t PING_MBULK -n 1000000 -c 1 -P 5000 -q

echo "redis pipeline test result:"
$BENCH_BIN -h $HOST_redis -p $PORT_redis -t set -n 1000000 -c 1 -P $BATCH_NUM -q