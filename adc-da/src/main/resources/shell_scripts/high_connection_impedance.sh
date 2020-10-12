#!/bin/bash

db=warningplatform

# 连接阻抗大模型算法
# 定义阈值参数
rth1=8
rth2=2
# 脚本参数：vin,startTime,endTime
vin=$1
startTime=$2
endTime=$3


sql="
with
ods_data as    -- 从ods层解析数据
(
  select
    get_json_object(data,'$.vin') vin,
    get_json_object(data,'$.msgTime') msgTime,
    cast (substring(get_json_object(data,'$.soc'),0,length(get_json_object(data,'$.soc'))-1) as double)/100 soc,
    cast (get_json_object(data,'$.differenceCellVoltage') as double)  differenceCellVoltage,
    cast (get_json_object(data,'$.totalCurrent') as double)  totalCurrent
  from ${db}.ods_preprocess_vehicle_data
  where dt >= date_format('${startTime}','yyyy-MM-dd')
  and   dt <= date_format('${endTime}','yyyy-MM-dd')
  and   get_json_object(data,'$.vin') = '${vin}'
  and   get_json_object(data,'$.msgTime') >= '${startTime}'
  and   get_json_object(data,'$.msgTime') <= '${endTime}'
  order by msgTime asc
),
pre_data as
(
  select
    vin,
    collect_list(soc) as soc,
    collect_list(differenceCellVoltage) as differenceCellVoltage,
    collect_list(totalCurrent) as totalCurrent
  from ods_data
  group by vin
)

insert into

"
hive  -e "${sql}"