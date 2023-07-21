#!/bin/bash

ARCH=`uname -m`
FLEX_HOME='/usr/local'

g++ -std=c++17 -flto -O3 -I${FLEX_HOME}/include -I/usr/lib/${ARCH}-linux-gnu/openmpi/include -o test_acid test_acid.cc -L${FLEX_HOME}/lib -lflex_utils -lflex_rt_mutable_graph -lflex_graph_db -lglog -lmpi -lmpi_cxx -pthread -lyaml-cpp
