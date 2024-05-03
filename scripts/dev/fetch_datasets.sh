#!/bin/bash

cat accession.list | while read ACCESSION; do
  datasets summary genome accession $ACCESSION
done