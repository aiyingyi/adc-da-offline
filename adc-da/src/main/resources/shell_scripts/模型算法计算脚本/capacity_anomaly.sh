#!/bin/bash

db=warningplatform

# 单体内阻或者容量异常模型算法，每个充放电循环完成一次
# vin,充放电时间端点

vin=$1
charge_start=$2
charge_end=$3
discharge_start=$4
discharge_end=$5

# 计算电芯的个数
th1=3
# 充电电芯占比
th2=50
# 放电电芯占比
th3=50

sql="
with
--  获取充电数据
charge_data as
(
    select
        get_json_object(data,'$.vin') vin,
        get_json_object(data,'$.msgTime') msgTime,
        get_json_object(data,'$.cellVoltage') cellVoltage
    from ${db}.ods_preprocess_vehicle_data
    where dt >= date_format('${charge_start}','yyyy-MM-dd')
    and   dt <= date_format('${charge_end}','yyyy-MM-dd')
    and   get_json_object(data,'$.vin') = '${vin}'
    and   get_json_object(data,'$.msgTime') >= '${charge_start}'
    and   get_json_object(data,'$.msgTime') <= '${charge_end}'
    order by msgTime asc
),
discharge_data as  -- 放电数据
(
    select
        get_json_object(data,'$.vin') vin,
        get_json_object(data,'$.msgTime') msgTime,
        get_json_object(data,'$.cellVoltage') cellVoltage
    from ${db}.ods_preprocess_vehicle_data
    where dt >= date_format('${discharge_start}','yyyy-MM-dd')
    and   dt <= date_format('${discharge_end}','yyyy-MM-dd')
    and   get_json_object(data,'$.vin') = '${vin}'
    and   get_json_object(data,'$.msgTime') >= '${discharge_start}'
    and   get_json_object(data,'$.msgTime') <= '${discharge_end}'
    order by msgTime asc
),
--  统计充放电工况下的全部电压
vol_info as
(
  select
    c.vin,
    c.vols as cvols,
    d.vols as dvols
  from (select vin,collect_list(cellVoltage) vols from charge_data group by vin) c
  join (select vin,collect_list(cellVoltage) vols from discharge_data group by vin) d
  on c.vin = d.vin
)

insert into table ${db}.capacity_anomaly_es
select
  vin,
  '${charge_start}',
  '${charge_end}',
  '${discharge_start}',
  '${discharge_end}',
  ${db}.capacity_anomaly(cvols,dvols,cast('${th1}' as int),cast('${th2}' as double),cast('${th3}' as double))
from vol_info
"
hive  -e "${sql}"