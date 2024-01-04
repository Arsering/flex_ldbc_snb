#!/bin/bash

ARCH=`uname -m`
FLEX_HOME=/usr/local
# FLEX_HOME=/root/flex_home

for i in n_is1 n_is2 n_is3 n_is4 n_is5 n_is6 n_is7
do
  g++ -flto -fno-gnu-unique -fPIC -g --std=c++17 -I/usr/lib/${ARCH}-linux-gnu/openmpi/include -I${FLEX_HOME}/include -L${FLEX_HOME}/lib -rdynamic -O3 -o lib${i}.so ${i}.cc -lflex_utils -lflex_rt_mutable_graph -lflex_graph_db -shared
  if [ $? -ne 0 ]; then
      echo ${i}: failed
      exit
  else
      echo ${i}: succeed
  fi
done
