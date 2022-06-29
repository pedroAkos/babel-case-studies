#!/bin/bash

#file=$1
#dst=$2

for n in $(uniq $OAR_FILE_NODES); do
  oarsh $n -n 'for d in $(docker ps -q); do docker cp  ./docker/network/log4j2.xml ${d}:./; done'
done


