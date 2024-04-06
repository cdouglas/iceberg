#!/bin/bash
# This script is used to generate TPC-DS data

# Check if the scale factor is provided
if [ $# -lt 1 ]; then
  echo "Usage: ./tpcds_data_gen.sh <scale_factor> <maintenance_streams>"
  exit 1
fi
if [ $# -gt 2 ]; then
  MAINTENANCE_STREAMS=$2
else
  MAINTENANCE_STREAMS=20
  echo "Using default maintenance streams: $MAINTENANCE_STREAMS"
fi
SF=$1
cd /home/spark/tpcds-kit/tools
DIR=/opt/spark/data/tpcds/sf_${SF}
mkdir -p ${DIR}

# LST-bench expects the data to be in CSV format and have no file extension
./dsdgen -dir $DIR -scale $SF -delimiter "|" -force -suffix "" -terminate n &

# Generate the maintenance data
for MAINTENANCE_STREAM in $(seq 1 $MAINTENANCE_STREAMS); do
  mkdir -p ${DIR}/${MAINTENANCE_STREAM}
  ./dsdgen -dir $DIR/${MAINTENANCE_STREAM} -scale $SF -delimiter "|" -force -suffix "" -terminate n -update $MAINTENANCE_STREAM &
done

wait