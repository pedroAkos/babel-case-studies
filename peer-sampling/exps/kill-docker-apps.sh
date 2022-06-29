#!/bin/bash

processes=$1

if [ -z $processes ] || [ $processes -lt 1 ]; then
  echo "please indicate a number of processes of at least one"
  exit 0
fi

nkill=$2
tokill=$(seq 0 $processes | shuf -n $nkill)

echo "killing nodes: $tokill"

cmd=""
for k in $tokill; do
    cmd="${cmd}docker exec node-${k} pkill java; "
done

for n in $(uniq $OAR_FILE_NODES); do
  oarsh $n -n $cmd
done


