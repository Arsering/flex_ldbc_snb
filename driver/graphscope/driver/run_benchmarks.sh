#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ..

server_ip="172.31.95.14"
sleep_dur=200
target_dir=/disk2

mkdir -p ${target_dir}

java -Xmx350g -Xms300g -cp target/graphscope-1.2.0-SNAPSHOT.jar org.ldbcouncil.snb.driver.Client  -P driver/benchmark.properties

echo "# Performance" > ./Performance.md
python3 ../../scripts/result_log_processor.py ./results sf30 >> ./Performance.md

curl -d "XXX" -X POST ${server_ip}:10000/interactive/exit
cp -r results ${target_dir}/sf30-results

sleep ${sleep_dur}

java -Xmx350g -Xms300g -cp target/graphscope-1.2.0-SNAPSHOT.jar org.ldbcouncil.snb.driver.Client  -P driver/benchmark-sf100.properties

python3 ../../scripts/result_log_processor.py ./results sf100 >> ./Performance.md

curl -d "XXX" -X POST ${server_ip}:10000/interactive/exit

cp -r results ${target_dir}/sf100-results

sleep ${sleep_dur}

java -Xmx350g -Xms300g -cp target/graphscope-1.2.0-SNAPSHOT.jar org.ldbcouncil.snb.driver.Client  -P driver/benchmark-sf300.properties

python3 ../../scripts/result_log_processor.py ./results sf300 >> ./Performance.md
 
curl -d "XXX" -X POST ${server_ip}:10000/interactive/exit

cp -r results ${target_dir}/sf300-results

mv ./Performance.md ../../
