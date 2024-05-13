#!/bin/bash

source .env

jsonl=$1

# Check if $jsonl is a valid file
if [ ! -f "$jsonl" ]; then
    echo "Error: $jsonl is not a file"
    exit 1
fi

root=$2

# Check if $root is a valid directory
if [ ! -d "$root" ]; then
    echo "Error: $root is not a directory"
    exit 1
fi

date=$(date +%Y-%m)


while IFS= read -r line; do
    accession=$(echo "$line" | jq -r '.reports[0] | .accession')
    
    name=$(echo "$line" | jq -r '.reports[0] | .organism.organism_name')
    echo "$line" | jq -r '.reports[0] | {"assembly_name": .assembly_info.assembly_name, "taxon_id": .organism.tax_id, "scientific_name": .organism.organism_name}' > $accession.assembly_info.json
    s3cmd put setacl --acl-public $accession.assembly_info.json s3://molluscdb/$date/$accession/assembly_info.json
    rm $accession.assembly_info.json
    if [[ "$accession" == GC* ]]; then
        dir=$root/${name// /_}
        echo $dir
        for arg in "${@:2}"; do
            if [ -d "$dir" ] && [ -f "$dir/run_$arg/short_summary.txt" ]; then
                ./scripts/raw-to-s3.py \
                    -c scripts/config/busco.yaml \
                    -d $dir \
                    -b molluscdb \
                    -p $date \
                    -u https://cog.sanger.ac.uk \
                    --vars accession=$accession lineage=$arg
            fi
        done
    fi
done < "$jsonl"


