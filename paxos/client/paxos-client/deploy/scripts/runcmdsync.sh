#!/bin/bash

cmd=$1
nodes=`./nodes.sh`

for node in $nodes
do
	#echo $node
        oarsh -o "StrictHostKeyChecking no" $node "$cmd" 2>&1 | sed "s/^/[$node] /"
done
echo done!