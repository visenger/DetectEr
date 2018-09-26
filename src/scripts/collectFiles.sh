#!/bin/bash

for d in */ ; do
	cd $d
    echo ${PWD##*/}
    mv part-00000-*.csv ${PWD##*/}.csv
    mv ${PWD##*/}.csv ../
    rm -r $d
    cd ..
done