#!/bin/bash

processes=$1

if [ -z $processes ] || [ $processes -lt 1 ]; then
  echo "please indicate a number of processes of at least one"
  exit 0
fi

i=0
base_port=5000



java -Xmx128m -DlogFilename=logs/node$((base_port + $i)) -cp peer-sampling.jar Main \
    -conf config.properties address=localhost port=$((base_port + $i)) \
    2>&1 | sed "s/^/[$((base_port + $i))] /" &
echo "launched process on port $((base_port + $i))"
sleep 0.5
contact="localhost:$((base_port + $i))"
i=$(($i + 1))

while [ $i -lt $processes ]; do
  java -Xmx128m -DlogFilename=logs/node$((base_port + $i)) -cp peer-sampling.jar Main \
    -conf config.properties address=localhost port=$((base_port + $i)) \
    contact=${contact} 2>&1 | sed "s/^/[$((base_port + $i))] /" &
  echo "launched process on port $((base_port + $i))"
  sleep 0.5
  contact="localhost:$((base_port + $i))"
  i=$(($i + 1))
done

sleep 2
read -p "------------- Press enter to kill servers. --------------------"

kill $(ps aux | grep 'peer-sampling.jar' | awk '{print $2}')

echo "All processes done!"
