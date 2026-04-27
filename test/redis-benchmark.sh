#!/bin/bash

HOST=$1
PORT=$2
REQS=100000
CLIENTS=1
RAND=100000
BENCH_BIN="/home/redis-8.0.0/src/redis-benchmark"
# array
$BENCH_BIN -h $HOST -p $PORT -c $CLIENTS -n 1000 -r $RAND -q SET K:__rand_int__ V:__rand_int__
# rbtree
$BENCH_BIN -h $HOST -p $PORT -c $CLIENTS -n $REQS -r $RAND -q RSET RK:__rand_int__ RV:__rand_int__
# hash
$BENCH_BIN -h $HOST -p $PORT -c $CLIENTS -n $REQS -r $RAND -q HSET HK:__rand_int__ HV:__rand_int__
# bitmap
$BENCH_BIN -h $HOST -p $PORT -c $CLIENTS -n $REQS -r 10000 -q BITSET bitkey __rand_int__ 1
# set
$BENCH_BIN -h $HOST -p $PORT -c $CLIENTS -n $REQS -r $RAND -q SSET skey member:__rand_int__
# sorted set
$BENCH_BIN -h $HOST -p $PORT -c $CLIENTS -n $REQS -r $RAND -q SSADD sskey member:__rand_int__ __rand_int__