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
        get_json_object(data,'$.cellVoltage') cellVoltage
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
  tmp.vin,
  '${startTime}',
  '${endTime}',
  tmp.avg_vol,
  tmp.std_vol
from
(
  select
    vin,
    ${db}.cell_vol_highdis(collect_list(cellVoltage),cast('${th1}' as int),cast('${th2}' as int)) as iswarning,
    ${db}.vol_avg_std(collect_list(cellVoltage),'avg') as avg_vol,
    ${db}.vol_avg_std(collect_list(cellVoltage),'std') as std_vol
  from ods_data
  group by vin
) as tmp
where tmp.iswarning = '1';


-- 将发生的预警信息写入到预警索引里面
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
  '单体电压离散度高',
  '单体电压离散度高',
  '1', --  审核状态
  null,
  null
from  (select vin from ${db}.cell_vol_highdis_es where vin = '${vin}' and startTime = '${startTime}' ) as r
join (
   select
        get_json_object(data,'$.vin') vin,
        get_json_object(data,'$.province') province,
        get_json_object(data,'$.vehicleType') as vehicleType,
        get_json_object(data,'$.enterprise') as enterprise
  from ${db}.ods_preprocess_vehicle_data
  where dt = date_format('${startTime}','yyyy-MM-dd')
  and get_json_object(data,'$.msgTime') = '${startTime}'
  and get_json_object(data,'$.vin') = '${vin}'
)  as other_info on r.vin = other_info.vin
join (
  select
    vin,
    licensePlate,
    battery_type
  from ${db}.vehicle_base_info
  where vin  = '${vin}'
)
 as vehicle_base on r.vin = vehicle_base.vin;


"
hive  -e "${sql}"