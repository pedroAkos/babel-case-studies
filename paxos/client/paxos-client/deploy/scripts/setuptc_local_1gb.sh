#!/bin/bash
if [[ -z "$1" ]]; then
  echo "Usage: setup_delay.sh [nServers]"
  exit
fi

RED='\033[0;31m'
BLUE='\033[0;34m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

n_Servers=$1
n_Nodes=$((n_Servers))

all_nodes=$(./nodes.sh)

declare -a nodes

idx=0
for i in $all_nodes; do
  if [ $idx -eq "$n_Nodes" ]; then
    break
  fi
  nodes+=($i)
  idx=$((idx + 1))
done

if [ $idx -lt "$n_Nodes" ]; then
  echo -e "${RED}Not enough nodes: Got $idx, required ${n_Nodes}$NC"
  exit
fi

echo -e "${GREEN}Nodes: $NC${nodes[*]}"

echo Setting up TC...

print_and_exec() {
  node=$1
  cmd=$2
  echo -e "$GREEN -- $node -- $NC$cmd"
  oarsh -n "$node" "$cmd" 2>&1 | sed "s/^/[$node] /"
}

download=1000
upload=1000

for node in "${nodes[@]}"; do
  echo -e "${RED} ${node}----------------------------------------------------------------------${NC}"

  echo -e "${BLUE}DOWNLOAD $download$NC"
  print_and_exec "${node}" "sudo-g5k modprobe ifb numifbs=1"
  print_and_exec "${node}" "sudo-g5k ip link add ifb0 type ifb"
  print_and_exec "${node}" "sudo-g5k ip link set dev ifb0 up"
  print_and_exec "${node}" "sudo-g5k tc qdisc add dev br0 handle ffff: ingress"
  print_and_exec "${node}" "sudo-g5k tc filter add dev br0 parent ffff: protocol ip u32 match u32 0 0 action mirred egress redirect dev ifb0"
  print_and_exec "${node}" "sudo-g5k tc qdisc del dev ifb0 root"
  print_and_exec "${node}" "sudo-g5k tc qdisc add dev ifb0 root handle 1: htb default 1"
  print_and_exec "${node}" "sudo-g5k tc class add dev ifb0 parent 1: classid 1:1 htb rate ${download}mbit"

  echo -e "${BLUE}UPLOAD $upload$NC"
  print_and_exec "${node}" "sudo-g5k tc qdisc del dev br0 root; sudo-g5k tc qdisc add dev br0 root handle 1: htb default 1"
  cmd="sudo-g5k tc class add dev br0 parent 1: classid 1:1 htb rate ${upload}mbit"
  print_and_exec "${node}" "$cmd"
done
