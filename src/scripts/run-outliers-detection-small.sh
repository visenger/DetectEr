#!/usr/bin/env bash

cd /Users/visenger/PycharmProjects/dBoost/dboost
STRATEGY=histogram

    for i in 0.6 0.7 0.8 0.9 0.95 0.99
	   do
	   for j in 0.001 0.005 0.01 0.02 0.03
            do
            echo "-new evaluation round- for $STRATEGY $i $j"
	   		RESULTFILE=/Users/visenger/research/datasets/BlackOak/Archive/outlier-output/out-DBoost-histogram-$i-$j.txt
	   		 ./dboost-stdin.py -F ,  --$STRATEGY $i $j --discretestats 8 2 -d string_case /Users/visenger/research/datasets/BlackOak/Archive/small-inputDB.csv > $RESULTFILE
			sed -i -e  1,2d $RESULTFILE
	   		done
       done

echo $?