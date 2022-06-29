#!/bin/bash

function recompile {
    mvn clean
    mvn package
}

recompile

version=0.0.1

cp peer-sampling.jar docker/network/peer-sampling.jar