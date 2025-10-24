# !/bin/bash

DATA_DIR="data-tpc-ds-sf1"
SCALE_FACTOR=1

mkdir -p $DATA_DIR
./dsdgen-tpc-ds -scale $SCALE_FACTOR -dir $DATA_DIR
