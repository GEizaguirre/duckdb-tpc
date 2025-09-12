# !/bin/bash

DATA_DIR="data-sf1"
SCALE_FACTOR=1

mkdir -p $DATA_DIR
./dsdgen -scale $SCALE_FACTOR -dir $DATA_DIR
