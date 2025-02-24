# Acdis

Acdis is a work-in-progress, distributed, redis-compatible database based on the actor model.

## Usage

To start an initial host along with a redis-compatible endpoint listening on `localhost:6379` run
```bash
cargo run --bin acdis
```

To join additional nodes to the cluster, run 
```bash
cargo run --bin acdis
```
