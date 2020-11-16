#!/bin/bash

db=warningplatform

# 单体电压波动性差异大  放电状态执行一次

# 脚本参数：vin,startTime,endTime
vin=$1
startTime=`date -d @$(($2/1000)) +'%Y-%m-%d %H:%M:%S'`
endTime=`date -d @$(($3/1000)) +'%Y-%m-%d %H:%M:%S'`
th1=0.16666


sql="
with
--  获取预处理的历史数据
ods_data as
(
    select
        get_json_object(data,'$.vin') vin,
        get_json_object(data,'$.msgTime') msgTime,
        cast(get_json_object(data,'$.maxCellVoltage') as double) maxCellVoltage,
        cast(get_json_object(data,'$.minCellVoltage') as double) minCellVoltage
    from ${db}.ods_preprocess_vehicle_data
    where dt >= date_format('${startTime}','yyyy-MM-dd')
    and   dt <= date_format('${endTime}','yyyy-MM-dd')
    and   get_json_object(data,'$.vin') = '${vin}'
    and   get_json_object(data,'$.msgTime') >= '${startTime}'
    and   get_json_object(data,'$.msgTime') <= '${endTime}'
    order by msgTime asc
),
-- 获取省份信息以及车型等其他信息
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
res as
(
  select
    vin,
    ${db}.vol_fluctuation(collect_list(maxCellVoltage),collect_list(minCellVoltage),cast('${th1}' as double)) as iswarning
  from ods_data
  group by vin
)

insert into table ${db}.battery_warning_info_es
select
  r.vin,
  other_info.vehicleType,
  other_info.enterprise,
  vehicle_base.licensePlate,
  vehicle_base.battery_type,
  '1',
  other_info.province,
  '${startTime}',
  '${endTime}',
  '单体电压波动性差异大',
  '单体电压波动性差异大',
  '1', --  审核状态
  null,
  null
from  (select vin from res where iswarning = '1' ) as r
join other_info on r.vin = other_info.vin
join vehicle_base on r.vin = vehicle_base.vin
"
hive  -e "${sql}"