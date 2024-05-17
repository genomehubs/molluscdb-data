#!/bin/bash

# ./pip_install_latest.sh macosx_10_9_x86_64

# genomehubs parse --directory ../lepbase-data/sources/raw_features/ --outfile tests/integration_tests/data/boat/assembly/lepidoptera.busco.tsv --window-size 1 --window-size 1000000


# curl -X DELETE "http://localhost:9200/taxon-*"
# curl -X DELETE "http://localhost:9200/a*"
# curl -X DELETE "http://localhost:9200/f*"
# curl -X DELETE "http://localhost:9200/s*"
curl -X DELETE "http://localhost:9202/*2021.10.15*"

genomehubs init \
    --config-file tests/integration_tests/config/molluscdb.yaml \
    --taxonomy-source ncbi \
    --taxonomy-root 2759 &&
genomehubs index \
    --config-file tests/integration_tests/config/molluscdb.yaml \
    --taxonomy-source ncbi \
    --assembly-dir tests/integration_tests/sources/assembly-data &&
# genomehubs index \
#     --config-file tests/integration_tests/config/molluscdb.yaml \
#     --taxonomy-source ncbi \
#     --feature-dir tests/integration_tests/sources/assembly-features &&
genomehubs fill \
    --config-file tests/integration_tests/config/molluscdb.yaml \
    --taxonomy-source ncbi \
    --traverse-root 2759 \
    --traverse-infer-both &&
echo done ||
echo failed
