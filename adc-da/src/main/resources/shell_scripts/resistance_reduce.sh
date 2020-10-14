#!/bin/bash

db=warningplatform

# 绝缘电阻执行脚本，每行驶结束或者充电结束执行一次

# 脚本参数：vin,startTime,endTime
vin=$1
startTime=$2
endTime=$3

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
        cast(get_json_object(data,'$.resistance') as double) resistance
    from ${db}.ods_preprocess_vehicle_data
    where dt >= date_format('${startTime}','yyyy-MM-dd')
    and   dt <= date_format('${endTime}','yyyy-MM-dd')
    and   get_json_object(data,'$.vin') = '${vin}'
    and   get_json_object(data,'$.msgTime') >= '${startTime}'
    and   get_json_object(data,'$.msgTime') <= '${endTime}'
    order by msgTime asc
)
insert into table resistance_reduce_es
select
    vin,
    r_reduce(collect_list(resistance),cast('${th}' as int),cast('${win}' as int)),
    '${startTime}',
    '${endTime}'
from ods_data
group by vin

"
hive  -e "${sql}"