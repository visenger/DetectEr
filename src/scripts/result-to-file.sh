#!/bin/bash

path=$(pwd)
psql -d error_detection_hosp -c "COPY (SELECT tid, attr, value, CASE WHEN label = TRUE  THEN 1.0 ELSE 0.0 END as prediction FROM error) To STDOUT CSV DELIMITER ',';"  > $path/result/error_detection_$1.csv
echo $?
