#!/bin/bash

db=warningplatform

# bms采样异常模型，每行驶结束或者充电结束执行一次

# 脚本参数：vin,startTime,endTime
vin=$1
startTime=$2
endTime=$3

# 不需要传入工况参数，根据平均压差判断是充电还是放电

th1=50
th2=80

sql="
with
--  获取预处理的历史数据
ods_data as
(
    select
        get_json_object(data,'$.vin') vin,
        get_json_object(data,'$.msgTime') msgTime,
        cast(get_json_object(data,'$.differenceCellVoltage') as double)*1000 differenceCellVoltage,
        cast(get_json_object(data,'$.maxCellVoltageNum') as int) maxCellVoltageNum,
        cast(get_json_object(data,'$.minCellVoltageNum') as int) minCellVoltageNum
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
-- 计算预警结果
res as
(
  select
    vin,
    ${db}.bms_sampling(collect_list(maxCellVoltageNum),collect_list(minCellVoltageNum),avg(differenceCellVoltage),cast('${th1}' as int),cast('${th2}' as int)) as iswarning
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
  '3',
  other_info.province,
  '${startTime}',
  '${endTime}',
  'BMS采样异常',
  'BMS采样异常',
  '1',
  null,
  null
from  (select vin from res where iswarning = '1' ) as r
join other_info on r.vin = other_info.vin
join vehicle_base on r.vin = vehicle_base.vin

"
hive  -e "${sql}"