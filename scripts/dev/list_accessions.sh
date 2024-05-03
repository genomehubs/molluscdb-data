#!/bin/bash

source .env

root=$1

# Check if $root is a valid directory
if [ ! -d "$root" ]; then
    echo "Error: $root is not a directory"
    exit 1
fi

for dir in $root/*; do
    if [ -d "$dir" ]; then
        for arg in "${@:2}"; do
            if [ -e $dir/run_$arg/short_summary.json ]; then
                temp=$(basename $(jq -r '.parameters.in' $dir/run_$arg/short_summary.json) | sed 's/_/__/2' | sed 's/\./__/2')
                if [[ "$temp" == GC* ]]; then
                    accession=${temp%%__*}
                    echo $accession
                fi
            fi
        done
    fi
done


