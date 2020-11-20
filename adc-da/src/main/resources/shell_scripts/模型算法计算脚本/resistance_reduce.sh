#!/bin/bash

db=warningplatform

# 绝缘电阻执行脚本，每行驶结束或者充电结束执行一次

# 脚本参数：vin,startTime,endTime
vin=$1
startTime=`date -d @$(($2/1000)) +'%Y-%m-%d %H:%M:%S'`
endTime=`date -d @$(($3/1000)) +'%Y-%m-%d %H:%M:%S'`

th=300
win=20

sql="
with
--  获取预处理的历史数据
ods_data as
(
    select
        get_json_object(data,'$.vin') vin,
        get_json_object(data,'$.msgTime') msgTime,
        cast(get_json_object(data,'$.insulationResistance') as double) resistance
    from ${db}.ods_preprocess_vehicle_data
    where dt >= date_format('${startTime}','yyyy-MM-dd')
    and   dt <= date_format('${endTime}','yyyy-MM-dd')
    and   get_json_object(data,'$.vin') = '${vin}'
    and   get_json_object(data,'$.msgTime') >= '${startTime}'
    and   get_json_object(data,'$.msgTime') <= '${endTime}'
    order by msgTime asc
),
-- 获取车辆的基础信息
vehicle_base as
(
  select
    vin,
    licensePlate,
    battery_type
  from ${db}.vehicle_base_info
  where vin  = '${vin}'
),
-- 获取省份等信息
other_info as
(
  select
        get_json_object(data,'$.vin') vin,
        get_json_object(data,'$.province') province,
        get_json_object(data,'$.vehicleType') as vehicleType,
        get_json_object(data,'$.enterprise') as enterprise
  from ${db}.ods_preprocess_vehicle_data
  where dt = date_format('${startTime}','yyyy-MM-dd')
  and get_json_object(data,'$.msgTime') = '${startTime}'
  and get_json_object(data,'$.vin') = '${vin}'
),
-- 计算预警结果
res as (
select
    vin,
    ${db}.r_reduce(collect_list(resistance),cast('${th}' as int),cast('${win}' as int)) as iswarning
from ods_data
group by vin)

insert into table ${db}.battery_warning_info_es
select
  r.vin,
  other_info.vehicleType,
  other_info.enterprise,
  vehicle_base.licensePlate,
  vehicle_base.battery_type,
  '中风险',
   other_info.province,
  '${startTime}',
  '${endTime}',
  '绝缘电阻突降',
  '绝缘电阻突降',
  '1',
  null,
  null
from  (select vin from res where iswarning = '1' ) as r
join other_info on r.vin = other_info.vin
join vehicle_base on r.vin = vehicle_base.vin


"
hive  -e "${sql}"