#!/bin/bash

db=warningplatform

# 单体电压波动性差异大  放电状态执行一次

# 脚本参数：vin,startTime,endTime
vin=$1
startTime=$2
endTime=$3

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
)

insert into table ${db}.vol_fluctuation_es
select
  vin,
  '${startTime}',
  '${endTime}',
  ${db}.vol_fluctuation(collect_list(maxCellVoltage),collect_list(minCellVoltage),cast('${th1} as double'))
from ods_data
group by vin;

"
hive  -e "${sql}"