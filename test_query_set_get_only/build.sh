#!/bin/bash

ARCH=`uname -m`
FLEX_HOME=/usr/local
# FLEX_HOME=/root/flex_home

# for i in n_is1 n_is2 n_is3 n_is4 n_is5 n_is6 n_is7

for i in n_ic1 n_ic2 n_ic3 n_ic4 n_ic5 n_ic6 n_ic7 n_ic8 n_ic9 n_ic10 n_ic11 n_ic12 n_ic13 n_ic14 n_is1 n_is2 n_is3 n_is4 n_is5 n_is6 n_is7 n_ins1 n_ins2 n_ins3 n_ins4 n_ins5 n_ins6 n_ins7 n_ins8
# for i in n_ic5
do
  g++ -flto -fno-gnu-unique -fPIC -g --std=c++17 -I/usr/lib/${ARCH}-linux-gnu/openmpi/include -I${FLEX_HOME}/include -L${FLEX_HOME}/lib -rdynamic -O3 -o lib${i}.so ${i}.cc -lflex_utils -lflex_rt_mutable_graph -lflex_graph_db -shared
  if [ $? -ne 0 ]; then
      echo ${i}: failed
      exit
  else
      echo ${i}: succeed
  fi
done
