#!/bin/bash

mpirun -np 5 -hostfile my_hosts ./mpisort /mnt/sort10gb
