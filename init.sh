#!/usr/bin/env bash

rm -rf data
mkdir data
cd data
wget https://www.labri.fr/perso/bourqui/downloads/cours/AnaVis/brut.zip
wget https://www.labri.fr/perso/bourqui/downloads/cours/AnaVis/clean.tar.gz
tar -xf clean.tar.gz
unzip -xf brut.zip
cd ..
pip install -U python-dotenv
