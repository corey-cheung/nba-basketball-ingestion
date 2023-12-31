#!/bin/bash

set -eufo pipefail

echo "Setting up nba-basketball conda env"

# Config shell session to work with conda
. ~/miniconda3/etc/profile.d/conda.sh

conda deactivate
conda env remove -n nba-basketball
conda create -yqn nba-basketball python=3.10
conda activate nba-basketball
conda install \
	--channel conda-forge \
	--channel defaults \
	--quiet \
	--yes \
	requests \
    psycopg2
conda install \
	--channel conda-forge \
	--channel defaults \
	--quiet \
	--yes \
    black \
    pylint \
	isort

echo "Finished, now spin up your new conda environment with 'conda activate nba-basketball'"
