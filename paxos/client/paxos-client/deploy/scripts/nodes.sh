#!/bin/bash
nodes=$(uniq "$OAR_NODEFILE" | sed -n '1!p' | sort -t - -k 2 -g)

for n in $nodes
do
	IFS=. read -r node _ <<< "$n"
	echo "$node"
done