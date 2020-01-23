#!/usr/bin/env bash

rm -rf data
mkdir data
cd data
wget https://www.labri.fr/perso/bourqui/downloads/cours/AnaVis/clean.tar.gz
tar -xvf clean.tar.gz
#cd ..

wget https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data
wget https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.names
wget https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.test

wget https://raw.githubusercontent.com/CODAIT/redrock/master/twitter-decahose/src/main/resources/Location/worldcitiespop.txt.gz
gunzip worldcitiespop.txt.gz
cd ..
