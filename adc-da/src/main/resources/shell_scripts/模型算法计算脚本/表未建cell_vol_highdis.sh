#!/bin/bash

db=warningplatform

# 单体电压离散度高模型

# 脚本参数：vin,startTime,endTime
vin=$1
startTime=$2
endTime=$3

th1=20
th2=80

sql="
with
--  获取预处理的历史数据
ods_data as
(
    select
        get_json_object(data,'$.vin') vin,
        get_json_object(data,'$.msgTime') msgTime,
        get_json_object(data,'$.cellVoltage') cellVoltage,
    from ${db}.ods_preprocess_vehicle_data
    where dt >= date_format('${startTime}','yyyy-MM-dd')
    and   dt <= date_format('${endTime}','yyyy-MM-dd')
    and   get_json_object(data,'$.vin') = '${vin}'
    and   get_json_object(data,'$.msgTime') >= '${startTime}'
    and   get_json_object(data,'$.msgTime') <= '${endTime}'
    order by msgTime asc
)

===================================未完成    es索引建表，sql更改
insert into table ${db}.cell_vol_highdis_es
select
  vin,
  '${startTime}',
  '${endTime}',
  ${db}.cell_vol_highdis(collect_list(cellVoltage),cast('${th1}' as int),cast('${th2}' as int))
from ods_data
group by vin;

"
hive  -e "${sql}"