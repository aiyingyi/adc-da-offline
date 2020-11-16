#!/bin/bash

db=warningplatform

# 单体内阻或者容量异常模型算法，每个充放电循环完成一次
# vin,充放电时间端点

vin=$1
charge_start=`date -d @$(($2/1000)) +'%Y-%m-%d %H:%M:%S'`
charge_end=`date -d @$(($3/1000)) +'%Y-%m-%d %H:%M:%S'`
discharge_start=`date -d @$(($4/1000)) +'%Y-%m-%d %H:%M:%S'`
discharge_end=`date -d @$(($5/1000)) +'%Y-%m-%d %H:%M:%S'`

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
),
-- 预警结果计算
res as
(
  select
    vin,
    ${db}.capacity_anomaly(cvols,dvols,cast('${th1}' as int),cast('${th2}' as double),cast('${th3}' as double)) as iswarning
  from vol_info
)
-- 如果发生预警，将四个时间点插入到es
insert into table  ${db}.capacity_anomaly_es
select
  tmp.vin,
  '${charge_start}',
  '${charge_end}',
  '${discharge_start}',
  '${discharge_end}'
from (select vin from res where iswarning = '1'  ) as tmp;

-- 将发生的预警信息写入到预警索引里面
insert into table ${db}.battery_warning_info_es
select
  r.vin,
  other_info.vehicleType,
  other_info.enterprise,
  vehicle_base.licensePlate,
  vehicle_base.battery_type,
  '2',
  other_info.province,
  '${charge_start}',
  '${discharge_end}',
  '单体内阻或者容量异常',
  '单体内阻或者容量异常',
  '1', --  审核状态
  null,
  null
from  (select vin from ${db}.capacity_anomaly_es where vin = '${vin}' and chargeStart = '${charge_start}' ) as r
join (
   select
        get_json_object(data,'$.vin') vin,
        get_json_object(data,'$.province') province,
        get_json_object(data,'$.vehicleType') as vehicleType,
        get_json_object(data,'$.enterprise') as enterprise
  from ${db}.ods_preprocess_vehicle_data
  where dt = date_format('${discharge_start}','yyyy-MM-dd')
  and get_json_object(data,'$.msgTime') = '${discharge_start}'
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