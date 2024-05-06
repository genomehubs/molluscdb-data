#!/bin/bash

source .env

root=$1

# Check if $root is a valid directory
if [ ! -d "$root" ]; then
    echo "Error: $root is not a directory"
    exit 1
fi


find "$root" -type d -name "run*odb10" | while read -r dir; do
    if [ -f "$dir/short_summary.json" ]; then
        assembly_path=$(jq -r '.parameters.in' $dir/short_summary.json)
        temp=$(basename $assembly_path | sed 's/_/__/2' | sed 's/\./__/2')
        if [[ "$temp" == GC* ]]; then
            accession=${temp%%__*}
            scp $assembly_path btkdev:/volumes/data/molluscdb/sources/assemblies/$accession.fa
        fi
    fi
done

