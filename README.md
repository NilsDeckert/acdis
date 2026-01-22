# Acdis

Acdis is a work-in-progress, distributed, redis-compatible database based on the actor model.
This is part of my bachelor's thesis.

## Usage

To start an initial host along with a redis-compatible endpoint listening on `localhost:6379` run
```bash
cargo run
```

To join additional nodes to the cluster, run 
```bash
cargo run --bin node
```

You can also use the script in `./scripts/benchmark_acdis.sh` to spawn multiple local nodes in one go.
