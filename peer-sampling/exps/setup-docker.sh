#!/bin/bash

net=mynet
image=peersampling:babel
vol=/home/$(whoami)/peer-sampling/logs


docker swarm init
JOIN_TOKEN=$(docker swarm join-token manager -q)

host=$(hostname)
for node in $(oarprint host); do
  if [ $node != $host ]; then
    oarsh $node "docker swarm join --token $JOIN_TOKEN $host:2377"
  fi
done

docker network create -d overlay --attachable --subnet 172.10.0.0/16 $net
