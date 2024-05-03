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
            temp=$(basename $(jq -r '.parameters.in' $dir/run_$arg/short_summary.json) | sed 's/_/__/2')
            accession=${temp%__*}
            ./scripts/raw-to-s3.py \
                -c scripts/config/busco.yaml \
                -d $dir \
                -b molluscdb \
                -p latest \
                -u https://cog.sanger.ac.uk \
                --vars accession=$accession lineage=$arg
        done
    fi
done


