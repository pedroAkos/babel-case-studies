#!/bin/bash

processes=$1
net=mynet
image=peersampling:babel
vol=/home/$(whoami)/peer-sampling/logs

if [ -z $processes ] || [ $processes -lt 1 ]; then
  echo "please indicate a number of processes of at least one"
  exit 0
fi

i=0
name="node"
while [ $i -lt $processes ]; do
  docker run --rm -v ${vol}:/code/logs -d -t --cap-add=NET_ADMIN --net $net --ip 172.10.2.$i --name $name-${i} -h $name-${i} $image $i
  i=$((i +1))
done

i=0

docker exec -d $name-${i} ./start.sh ""
i=$(($i + 1))

contact="$name-${i}:5000"

while [ $i -lt $processes ]; do
  docker exec -d $name-${i} ./start.sh $contact
  i=$((i +1))
done

echo "All processes done!"
