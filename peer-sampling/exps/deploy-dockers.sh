#!/bin/bash

processes=$1
net=mynet
image=peersampling:babel
vol=/home/$(whoami)/peer-sampling/logs
n_nodes=$(uniq $OAR_FILE_NODES | wc -l)
function nextnode {
  local idx=$(($1 % n_nodes))
  local i=0
  for host in $(uniq $OAR_FILE_NODES); do
    if [ $i -eq $idx ]; then
      echo $host
      break;
    fi
    i=$(($i +1))
  done
}



i=0
name="node"
while [ $i -lt $processes ]; do
  node=$(nextnode $i)
  oarsh $node -n "docker run --rm -v ${vol}:/code/logs -d -t --cap-add=NET_ADMIN --net $net --ip 172.10.2.$i --name $name-${i} -h $name-${i} $image $i"
  i=$((i +1))
done
echo "All dockers launched!"



