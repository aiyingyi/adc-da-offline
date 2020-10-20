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
--  自定义udf函数：vol_avg_std():计算均值和方差 ,cell_vol_highdis()：输出预警
insert into table ${db}.cell_vol_highdis_es
select
  vin,
  '${startTime}',
  '${endTime}',
  ${db}.cell_vol_highdis(collect_list(cellVoltage),cast('${th1}' as int),cast('${th2}' as int)),
  ${db}.vol_avg_std(collect_list(cellVoltage),'avg'),
  ${db}.vol_avg_std(collect_list(cellVoltage),'std')
from ods_data
group by vin;

"
hive  -e "${sql}"