#!/usr/bin/env sh
sudo nice -n -20 numactl --interleave 0,1,2,3,4,5,6,7 ./a.out
