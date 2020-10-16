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
)

insert into table ${db}.bms_sampling_es
select
  vin,
  '${startTime}',
  '${endTime}',
  ${db}.bms_sampling(collect_list(maxCellVoltageNum),collect_list(minCellVoltageNum),avg(differenceCellVoltage),cast('${th1}' as int),cast('${th2}' as int))
from ods_data
group by vin;

"
hive  -e "${sql}"