#!/bin/bash

ARCH=`uname -m`
# FLEX_HOME=/usr/local
FLEX_HOME=/root/flex_home

for i in ic1 ic2 ic3 ic4 ic5 ic6 ic7 ic8 ic9 ic10 ic11 ic12 ic13 ic14 is1 is2 is3 is4 is5 is6 is7 ins1 ins2 ins3 ins4 ins5 ins6 ins7 ins8
# for i in ic1
do
  g++ -flto -fno-gnu-unique -fPIC -g --std=c++17 -I/usr/lib/${ARCH}-linux-gnu/openmpi/include -I${FLEX_HOME}/include -L${FLEX_HOME}/lib -rdynamic -O3 -o lib${i}.so ${i}.cc -lflex_utils -lflex_rt_mutable_graph -lflex_graph_db -shared
done
