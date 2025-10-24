# !/bin/bash

DATA_DIR="data-tpc-h-sf1"
SCALE_FACTOR=1

mkdir -p $DATA_DIR
./dbgen-tpc-h -s $SCALE_FACTOR
mv *.tbl $DATA_DIR
