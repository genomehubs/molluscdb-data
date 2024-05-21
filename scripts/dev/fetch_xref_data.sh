#!/bin/bash

ROOT=Mollusca

# Fetch UCSC accession list
curl -s https://hgdownload.soe.ucsc.edu/hubs/UCSC_GI.assemblyHubList.txt \
    | iconv -c -f ISO-8859-1 -t UTF-8//TRANSLIT -c \
    > UCSC_GI_assemblyHubList.tsv

# # Fetch Ensembl Fungi accession list
# curl -s http://ftp.ensemblgenomes.org/pub/current/fungi/species_metadata_EnsemblFungi.json \
#     | jq -r '.[] | [.assembly.assembly_accession, .organism.url_name, .data_release.release_date, .organism.strain, .organism.taxonomy_id] | @tsv' \
#     | gzip -c > species_metadata_EnsemblFungi.tsv.gz

# # Fetch Ensembl Metazoa accession list
# curl -s http://ftp.ensemblgenomes.org/pub/current/metazoa/species_metadata_EnsemblMetazoa.json \
#     | jq -r '.[] | [.assembly.assembly_accession, .organism.url_name, .data_release.release_date, .organism.strain, .organism.taxonomy_id] | @tsv' \
#     | gzip -c > species_metadata_EnsemblMetazoa.tsv.gz
# Fetch Ensembl Metazoa accession list
curl -s http://ftp.ensemblgenomes.org/pub/current/metazoa/species_EnsemblMetazoa.txt \
    | cut -f 1-6 \
    > EnsemblMetazoa_species_metadata.tsv

# # Fetch Ensembl Plants accession list
# curl -s http://ftp.ensemblgenomes.org/pub/current/plants/species_metadata_EnsemblPlants.json \
#     | jq -r '.[] | [.assembly.assembly_accession, .organism.url_name, .data_release.release_date, .organism.strain, .organism.taxonomy_id] | @tsv' \
#     | gzip -c > species_metadata_EnsemblPlants.tsv.gz

# # Fetch Ensembl Protists accession list
# curl -s http://ftp.ensemblgenomes.org/pub/current/protists/species_metadata_EnsemblProtists.json \
#     | jq -r '.[] | [.assembly.assembly_accession, .organism.url_name, .data_release.release_date, .organism.strain, .organism.taxonomy_id] | @tsv' \
#     | gzip -c > species_metadata_EnsemblProtists.tsv.gz

# Fetch Ensembl Rapid accession list
curl -s https://ftp.ensembl.org/pub/rapid-release/species_metadata.json \
    | jq -r '.[] | [.species, .ensembl_production_name, "EnsemblRapid", .taxonomy_id, .assembly_name, .assembly_accession] | @tsv' \
    > EnsemblRapid_species_metadata.tsv

# # Fetch Ensembl Vertebrates accession list
# curl -s https://ftp.ensembl.org/pub/current/species_metadata_EnsemblVertebrates.json \
#     | jq -r '.[] | [.assembly.assembly_accession, .organism.url_name, .data_release.release_date, .organism.strain, .organism.taxonomy_id] | @tsv' \
#     | gzip -c > species_metadata_EnsemblVertebrates.tsv.gz

# Fetch BoaT accession list
curl -s -X 'GET' \
    "https://boat.genomehubs.org/api/v2/search?query=tax_tree%28$ROOT%29&result=assembly&taxonomy=ncbi&fields=none&size=100000&queryId=mdb-import" \
    -H 'accept: text/tab-separated-values' \
    > boat_accession_list.tsv

# Fetch BTK accession list
curl -s -X 'GET' \
    "https://blobtoolkit.genomehubs.org/api/v1/search/$ROOT" \
    | jq -r '.[] | [.taxid, .taxon_name, .accession, .id] | @tsv' \
    > btk_accession_list.tsv