

set -o pipefail

ls /datasets/mmap_files/* -al | grep content | awk '{print $9}' | xargs rm -rf
ps aux | grep ldbc_snb | grep -v grep | awk '{print $2}' | xargs kill -9
../../build/ldbc_snb_interactive -g /datasets/luoxiaojian/sf_300_n.bin -s 48 &
sleep 480
