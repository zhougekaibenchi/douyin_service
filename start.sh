#!/bin/bash

environment='test'
cron_log="log/cron.log"
sever_log="log/sever.log"

echo "(1) Kill exist current task instance.........."
pids_cron=$(ps -ef | grep cron.py | grep -v "grep" | awk '{printf("%s ", $2)}')
for pid in ${pids_cron}; do
  kill -s 9 ${pid}
  echo "kill process ${pid} successfully"
done
pids_sever=$(ps -ef | grep unicorn | grep -v "grep" | awk '{printf("%s ", $2)}')
for pid in ${pids_sever}; do
  kill -s 9 ${pid}
  echo "kill process ${pid} successfully"
done
echo "kill successfully!"

echo "(2) Please wait restart..................."
#source /mengyuan
#cd /douyin/pipeline || exit
echo "Run sever job................"
nohup python -m sever.py $environment >> $sever_log 2>&1 &
echo "Restart cron job................"
nohup python -m cron.py $environment >> $cron_log 2>&1 &
echo "restart successfully!"

echo "(3) Run pipeline for check.........."
nohup python -m pipeline.py $environment >> $cron_log 2>&1 &


