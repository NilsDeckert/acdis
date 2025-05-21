#!/bin/bash

# Check if a valid number is provided
if [[ -z "$1" || "$1" -lt 1 || -z $2 ]]; then
  echo "Usage: $0 <number of total processes> <number_of_threads per process>"
  exit 1
fi

n="$1"
threads="$2"
pids=()
running=true

# Handle Ctrl+C to terminate all child processes
cleanup() {
  echo "Terminating..."
  for pid in "${pids[@]}"; do
    kill "$pid" 2>/dev/null
  done
  wait
  exit 0
}

trap cleanup SIGINT

watch_pids() {
  while $running; do
    for pid in "${pids[@]}"; do
      if ! kill -0 "$pid" 2>/dev/null; then
        echo "Process $pid exited. Shutting down all..."
        cleanup
      fi
    done
    sleep 1
  done
}

cargo build --release
cargo build --release --bin node

# Start the first main process
TOKIO_WORKER_THREADS=$threads cargo run --release &
pids+=($!)

# Start the remaining node processes
for ((i=1; i<n; i++)); do
  sleep 3
  TOKIO_WORKER_THREADS=$threads cargo run --release --bin node &
  pids+=($!)
done

watch_pids &

# Wait for all background processes
wait
