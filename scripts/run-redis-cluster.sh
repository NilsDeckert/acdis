#!/bin/bash

nodes=${1:-3}
name_max_nodes=$((7000+$nodes-1))

### Input validation
if [[ ! -e 7000/ ]]; then
  echo "Could not find dir 7000/. Make sure you are in the right directory"
  exit -1
fi

if [[ ! -e 7000/redis.conf ]]; then
  echo "Could not find file redis.conf in dir 7000/"
  exit -1
fi

### Copy/Create folders for each cluster node
for ((i=1;i<nodes;i++)); do
  newname=$((7000+$i))
  cp -r 7000/ $newname/
  sed -i "s/7000/$newname/" $newname/redis.conf
done

tmux new -s "redis-cluster" -d

for ((i=7000;i<=$name_max_nodes;i++)); do
  tmux send-keys -t "redis-cluster" "cd $i && redis-server ./redis.conf" ENTER;
  tmux split-window;
done

addresses=()
for ((i=7000;i<=$name_max_nodes;i++)); do
  addresses+=$(printf "127.0.0.1:%s " $i)
done

# After last iteration, init cluster
tmux send-keys -t "redis-cluster" "redis-cli --cluster create ${addresses[@]}" ENTER;
sleep 1
tmux send-keys -t "redis-cluster" "yes" ENTER;

# Attach to session
tmux a

### Cleanup
for ((i=7001;i<=$name_max_nodes;i++)); do
  rm -rf $i
done

rm 7000/nodes.conf
