#!/bin/bash

db=warningplatform

# 连接阻抗大模型算法
# 定义阈值参数
rth1=8
rth2=2
# 脚本参数：vin,startTime,endTime
vin=$1
startTime=`date -d @$(($2/1000)) +'%Y-%m-%d %H:%M:%S'`
endTime=`date -d @$(($3/1000)) +'%Y-%m-%d %H:%M:%S'`


sql="
with
ods_data as    -- 从ods层解析数据
(
  select
    get_json_object(data,'$.vin') vin,
    get_json_object(data,'$.msgTime') msgTime,
    cast (get_json_object(data,'$.soc') as double) soc,
    cast (get_json_object(data,'$.differenceCellVoltage') as double)*1000  differenceCellVoltage,
    cast (get_json_object(data,'$.totalCurrent') as double)  totalCurrent
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
      ${db}.conn_impedance(collect_list(differenceCellVoltage),collect_list(totalCurrent),collect_list(soc),
      cast('${rth1}' as double),cast('${rth2}' as double)) as iswarning
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
  '中风险',
  other_info.province,
  '${startTime}',
  '${endTime}',
  '连接阻抗大',
  '连接阻抗大',
  '1',
  null,
  null
from  (select vin from res where iswarning = '1' ) as r
join other_info on r.vin = other_info.vin
join vehicle_base on r.vin = vehicle_base.vin

"
hive  -e "${sql}"