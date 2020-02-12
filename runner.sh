#!/bin/bash

# This script is responsible for ...

# Hints
# https://devhints.io/bash


echo "Script runner.sh started"
echo "$(date +%d)/$(date +%m)/$(date +%Y) $(date +%H):$(date +%M):$(date +%S) $(date +%Z)"

FILENAME = $(echo "concurrent.c")

if [ ! -e ${FILENAME} ]; then
    echo "ERROR: File ${FILENAME} not found"
    exit 1
fi

#COMMAND = "gcc $FILENAME.c -pthread -lm -o $FILENAME.o";
COMMAND = $(which gcc);

for ((i = 0; i < 8; i++)); do
    echo "Triggering execution #${i} of ${FILENAME}"

    #TODO run filename
    echo "${COMMAND}"

done

# TODO acess files and compute average
# another c program can be called

echo "Script runner.sh finished"
