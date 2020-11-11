#!/bin/bash

db=warningplatform

# 获取前一小时时间
do_date=`date  "+%Y-%m-%d %H:%M:%S"`

if [[ -n "$1" ]]; then
    do_date="$1"
fi

start_time=`date -d "1 hour ago ${do_date}" "+%Y-%m-%d %H:%M:%S"`
day=`date -d "${start_time}" "+%Y-%m-%d"`

# 1. 从es导入数据阶段

# 2. 统计每一小时的数据
sql="
with
-- 上一个小时的故障数据
failure_info as
(
  select
      enterprise,
      vin,
      vehicleType,
      province,
      failureType,
      failureStartTime
  from ${db}.failure_es where date_format(failureStartTime,'yyyy-MM-dd HH') = date_format('${start_time}','yyyy-MM-dd HH')
)
insert into table ${db}.failure_statistics_perhour partition(day='${day}')
select
  enterprise,
  province,
  vehicleType,
  vin,
  failureType,
  count(failureType) as total,
  date_format('${start_time}','yyyy-MM-dd HH') as dt
from  failure_info
group by enterprise,province,vehicleType,vin,failureType;
-- 将计算结果保存到es
insert into table ${db}.failure_statistics_perhour_es
select
    enterprise,
    province,
    vehicleType,
    vin,
    failureType,
    total,
    dt,
from ${db}.failure_statistics_perhour
where day='${day}';
"

hive -e  "${sql}"





