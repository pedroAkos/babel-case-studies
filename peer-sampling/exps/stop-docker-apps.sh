#!/bin/bash

for n in $(uniq $OAR_FILE_NODES); do
  oarsh $n -n 'for d in $(docker ps -q); do docker exec $d pkill java; done'
done


