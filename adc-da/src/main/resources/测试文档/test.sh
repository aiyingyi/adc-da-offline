#!/bin/bash

db=warningplatform
window_size=10

sql="
with
charge_data as   -- 最近10次有效充电为且充电数据帧soc > 0.8 的数据
(
   select * from  ${db}.charge_data
),
charge_info as
(
  select
    vin,
    cast(charge_start_time as bigint) charge_start_time,
    avg(differenceCellVoltage) as avg_vol_diff
  from  charge_data group by vin,charge_start_time
),
charge_statistics as
(
  select
      base.vin,
      '${args[$[2*$window_size-2]]}' as  last_charge_time,
      collect_list(cast ((avg_vol_diff/(1000*3600*24)) as double)) as vol_diff,
      collect_list(day_diff) as day_diff
  from
      (select    --计算充电之间的时间差
        vin,
        avg_vol_diff,
        charge_start_time-LAG(charge_start_time,1,charge_start_time)  over(partition by vin order by charge_start_time asc)  as day_diff
      from charge_info) as base
  group by base.vin
)
select * from  charge_statistics

"
hive  -e "${sql}"