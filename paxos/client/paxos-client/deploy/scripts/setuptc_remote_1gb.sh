#!/bin/bash
if [[ -z "$2" ]]; then
  echo "Usage: setup_delay.sh [nServers] [latency_file]"
  exit
fi

RED='\033[0;31m'
BLUE='\033[0;34m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

n_Servers=$1
n_Nodes=$((n_Servers))
latency_file=$2

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

i=0
while read -r raw_line; do
  if [ $i -ge "$n_Nodes" ]; then
          exit
  fi
  echo -e "${RED} ${nodes[$i]}----------------------------------------------------------------------${NC}"
  echo -e "$RED Line: $NC$raw_line "
  if [[ $raw_line == \#* ]]; then
    echo "Ignored"
    continue
  fi
  j=0
  IFS=', ' read -r -a line <<<"$raw_line"

  download=${line[0]}
  upload=${line[1]}

  echo -e "${BLUE}DOWNLOAD $download$NC"
  print_and_exec "${nodes[$i]}" "sudo-g5k modprobe ifb numifbs=1"
  print_and_exec "${nodes[$i]}" "sudo-g5k ip link add ifb0 type ifb"
  print_and_exec "${nodes[$i]}" "sudo-g5k ip link set dev ifb0 up"
  print_and_exec "${nodes[$i]}" "sudo-g5k tc qdisc add dev br0 handle ffff: ingress"
  print_and_exec "${nodes[$i]}" "sudo-g5k tc filter add dev br0 parent ffff: protocol ip u32 match u32 0 0 action mirred egress redirect dev ifb0"
  print_and_exec "${nodes[$i]}" "sudo-g5k tc qdisc del dev ifb0 root"
  print_and_exec "${nodes[$i]}" "sudo-g5k tc qdisc add dev ifb0 root handle 1: htb default 1"
  print_and_exec "${nodes[$i]}" "sudo-g5k tc class add dev ifb0 parent 1: classid 1:1 htb rate ${download}mbit"

  echo -e "${BLUE}UPLOAD $upload$NC"
  print_and_exec "${nodes[$i]}" "sudo-g5k tc qdisc del dev br0 root; sudo-g5k tc qdisc add dev br0 root handle 1: htb"
  cmd="sudo-g5k tc class add dev br0 parent 1: classid 1:1 htb rate ${upload}mbit"
  print_and_exec "${nodes[$i]}" "$cmd"
  for n in "${line[@]:2}"; do
    if [ $i -ne $j ]; then
      if [ $j -ge "$n_Nodes" ]; then
        continue
      fi
      target_ip=$(getent hosts "${nodes[$j]}" | awk '{print $1}')
      echo -e "latency from ${GREEN}${nodes[$i]}${NC} to ${BLUE}${nodes[$j]}${NC} ($target_ip) is ${RED}${n}${NC}"
      cmd1="sudo-g5k tc class add dev br0 parent 1:1 classid 1:1$j htb rate 1mbit ceil 20000mbit && "
      cmd2="sudo-g5k tc qdisc add dev br0 parent 1:1$j netem delay ${n}ms $((n / 10))ms distribution normal && "
      cmd3="sudo-g5k tc filter add dev br0 protocol ip parent 1:0 prio 1 u32 match ip dst $target_ip flowid 1:1$j"
      print_and_exec "${nodes[$i]}" "$cmd1$cmd2$cmd3"
    fi
    j=$((j + 1))
  done
  i=$((i + 1))
done <"$latency_file"