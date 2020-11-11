#!/bin/bash

db=warningplatform

start_time=`date -d "1 day ago" "+%Y-%m-%d %H:%M:%S"`


if [[ -n "$1" ]]; then
    start_time="$1"
fi

day=`date -d "${start_time}" "+%Y-%m-%d"`

# 1. 从es导入数据阶段

# 2. 统计每一小时的数据
sql="
with
-- 获取前一天的报警数据
failure_info_day as
(
  select
      enterprise,
      province,
      vehicleType,
      vin,
      failureType,
      total
  from ${db}.failure_statistics_perhour  where  dt= '${day}'
)

-- 将计算结果保存到es
insert into table ${db}.failure_statistics_perday_es
select
  enterprise,
  province,
  vehicleType,
  vin,
  failureType,
  sum(total) as total,
  '${day}' as dt
from  failure_info_day
group by enterprise,province,vehicleType,vin,failureType;

"

hive -e  "${sql}"





