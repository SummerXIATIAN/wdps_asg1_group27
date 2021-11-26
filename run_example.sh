#!/bin/sh
echo "Setup Envirement ..."
sh setup.sh
echo "Processing webpages ..."
python3 code_query.py data/sample.warc.gz > sample_predictions.tsv
echo "Computing the scores ..."
python3 score.py data/sample_annotations.tsv sample_predictions.tsv
