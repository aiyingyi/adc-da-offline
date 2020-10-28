#!/bin/bash

db=warningplatform
# 电池包衰减预警模型,充电完成而且soc增量大于40%,执行一次
vin=$1

start_time=$2
end_time=$3

start_soc=$4
end_soc=$5
# 累计里程
odo=$6

sql="
with
-- 获取车辆充电时间内的原始数据
ods_data as
(
  select
      get_json_object(data,'$.vin') vin,
      get_json_object(data,'$.province') province,
      cast(get_json_object(data,'$.msgTime') as bigint) msgTime,
      cast(get_json_object(data,'$.totalCurrent') as double)  totalCurrent,
      cast(substring(get_json_object(data,'$.soc'),0,length(get_json_object(data,'$.soc'))-1) as double)/100  soc
  from ${db}.ods_preprocess_vehicle_data
  where dt>=date_format('${start_time}','yyyy-MM-dd')
  and dt<=date_format('${end_time}','yyyy-MM-dd')      --根据日期分区查找数据
  and get_json_object(data,'$.msgTime') >= '${start_time}'
  and get_json_object(data,'$.msgTime') <= '${end_time}'
  and get_json_object(data,'$.vin') = '${vin}'
),
-- 计算出初始数据
charge_data as
(
  select
    vin,
    totalCurrent,
    msgTime-lag(msgTime,1,msgTime)  over(partition by vin order by msgTime asc ) as timeDiff  -- 升序排列，获取时间差,第一行取0
  from ods_data
),
-- 计算出充电容量
charge_electricity  as
(
  select
    vin,
    sum(timeDiff/(1000*60*60 ) * totalCurrent)/('${end_soc}'-'${start_soc}') as chargeCapacity
  from  charge_data
  group by vin
)
--  ############################## todo  自定义预警判断函数
insert into table ${db}.battery_attenuation_es
select
  vin,
  cast('${charge_start}' as bigint),
  cast('${charge_end}' as bigint),
  cast('${odo}' as double),
  chargeCapacity,
  ${db}.battery_attenuation(cast('${odo}' as double),chargeCapacity)
from charge_electricity;



"
hive  -e "${sql}"