#!/bin/bash


echo "Stopping nodes"
for n in $(uniq $OAR_FILE_NODES); do
  oarsh $n -n 'docker kill $(docker ps -aq)'
done


