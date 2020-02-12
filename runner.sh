#!/bin/bash

# This script is responsible for automating the generation of average execution time of partitioning algorithms in memory. Each parameter passed to the file that hols the algorithms will generate a text file. The text file generated contains the average time.

# Hints
# https://devhints.io/bash


echo "Script runner.sh started"
echo "$(date +%d)/$(date +%m)/$(date +%Y) $(date +%H):$(date +%M):$(date +%S) $(date +%Z)"

FILENAME="concurrent.c"

if [ ! -e $FILENAME ]; then
    echo "ERROR: File ${FILENAME} not found"
    exit 1
fi

gcc concurrent.c -pthread -lm -o concurrent.o 

NUM_THREADS=('1' '2' '4' '8' '16' '32')

for val_t in "${NUM_THREADS[@]}"; do
    
    for val_h in {1..18}; do
        echo "#Threads: $val_t #Bits: $val_h"
        
        ./concurrent.o "$val_t" "$val_h"

    done

done

echo "Script runner.sh finished"
