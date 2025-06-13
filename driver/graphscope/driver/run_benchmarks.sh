#!/bin/bash
export CUR_DIR=$(pwd)
set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

# server_ip="172.24.177.243"
# sleep_dur=200
# target_dir=/mnt/nas/flex_results

# mkdir -p ${target_dir}

time=$(date "+%Y-%m-%d-%H:%M:%S")
LOG_DIR=${CUR_DIR}/logs/${time}/client
mkdir -p ${LOG_DIR}/shells

# store shell file
touch ${LOG_DIR}/shells/aaa
cp -r ${CUR_DIR}/$0 ${LOG_DIR}/shells/aaa
mv ${LOG_DIR}/shells/aaa ${LOG_DIR}/shells/${time}.sh

# run benchmark
java -Xmx100g -Xms100g -cp target/graphscope-1.2.0-SNAPSHOT.jar org.ldbcouncil.snb.driver.Client -P driver/benchmark-sf1000.properties -rd ${LOG_DIR}/ldbc_snb_results/ > ${LOG_DIR}/ldbc_snb_logs.log