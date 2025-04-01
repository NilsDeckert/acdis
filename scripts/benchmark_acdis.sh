#!/bin/bash

cargo build --release
cargo run --release &
MAIN_NODE=$!

cargo build --release --bin node
cargo run --release --bin node &
EXTRA_NODE1=$!
sleep 3

cargo run --release --bin node &
EXTRA_NODE2=$!
sleep 3

redis-benchmark -t set,get -r 10000 -n 500000 -c 5 --threads 5 --cluster

kill -2 $EXTRA_NODE2
kill -2 $EXTRA_NODE1
kill -2 $MAIN_NODE
