#!/bin/bash

processes=$1
logs=$2
shift 2

if [ -z $processes ] || [ $processes -lt 1 ]; then
  echo "please indicate a number of processes of at least one"
  exit 0
fi

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


name="node"
i=0
docker exec -d $name-${i} ./start.sh ${logs} ""
contact="$name-${i}:5000"
i=$(($i + 1))
while [ $i -lt $processes ]; do
  node=$(nextnode $i)
  sleep 0.5
  oarsh $node -n "docker exec -d $name-${i} ./start.sh ${logs} contact=$contact $@"
  i=$((i +1))
done


