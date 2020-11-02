#/bin/bash

load="workloads/load80G4kb.spec"
workloads="workloads/workloada.spec workloads/workloadb.spec workloads/workloadc.spec workloads/workloadd.spec workloads/workloade.spec workloads/workloadf.spec"

CLEAN_CACHE() {
    sleep 2
    sync
    echo 3 > /proc/sys/vm/drop_caches
    free -h >>out.out
    sleep 2
}

cmd="./ycsbc -db pmskiplist -threads 1 -P $load -load true -dbstatistics true -dbwaitforbalance false >>out.out 2>&1 "
echo $cmd >out.out
echo $cmd
eval $cmd
if [ $? -ne 0 ];then
    exit 1
fi

for file_name in $workloads; do
  CLEAN_CACHE
  cmd="./ycsbc -db pmskiplist -threads 1 -P $file_name -run true -dbstatistics true -dbwaitforbalance false >>out.out 2>&1 "
  echo $cmd >>out.out
  echo $cmd
  eval $cmd
  if [ $? -ne 0 ];then
      exit 1
  fi
  wait
done