#!/bin/bash

processes=$1

#./deploy-dockers.sh $processes

#sleep 1m

echo "Starting exps!"
for run in 1 2 3; do
  echo "Run $run"
  for faults in 0 10 25 50; do
    echo "Faults $faults"
    ./run-docker-apps.sh $processes "exp-A5-P10-${run}-${faults}" "ActiveView=5" "PassiveView=10"
    sleep 6m
    if [ $faults -gt 0 ]; then
      ./kill-docker-apps.sh $processes $faults
    fi
    sleep 6m
    ./stop-docker-apps.sh
    #mkdir "${logs}-$run-$faults"
    #docker run --rm -v ${logs}:/logs alpine chmod -R +x logs
    #mv ${exp}/* "${logs}-$run-$faults"
  done
done

