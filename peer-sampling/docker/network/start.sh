#!/bin/sh

exp=$1
shift 1
servernode=$(hostname)

mkdir logs/${exp}

java -Xmx128m -DlogFilename=logs/${exp}/$servernode -cp peer-sampling.jar Main \
    -conf config.properties $@ \
    2> logs/${exp}/$servernode.err

