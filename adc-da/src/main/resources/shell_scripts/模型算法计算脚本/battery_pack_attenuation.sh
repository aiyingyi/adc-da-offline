#!/bin/bash

db=warningplatform
# 电池包衰减预警模型,充电完成而且soc增量大于40%,执行一次

vin=$1
# 时间戳形式,将时间戳变成日期类型
start_time=`date -d @$(($2/1000)) +'%Y-%m-%d %H:%M:%S'`


end_time=`date -d @$(($3/1000)) +'%Y-%m-%d %H:%M:%S'`

# soc以整数形式
start_soc=$4
end_soc=$5
# 累计里程
odo=$6
# 定义阈值
th=1

sql="
with
-- 获取车辆充电时间内的原始数据
ods_data as
(
  select
      get_json_object(data,'$.vin') vin,
      get_json_object(data,'$.province') province,
      cast(unix_timestamp(get_json_object(data,'$.msgTime')) as bigint) msgTime,
      cast(get_json_object(data,'$.totalCurrent') as double)  totalCurrent,
      cast(get_json_object(data,'$.soc') as double)  soc
  from ${db}.ods_preprocess_vehicle_data
  where dt>=date_format('${start_time}','yyyy-MM-dd')
  and dt<=date_format('${end_time}','yyyy-MM-dd')      --根据日期分区查找数据
  and get_json_object(data,'$.msgTime') >= '${start_time}'
  and get_json_object(data,'$.msgTime') <= '${end_time}'
  and get_json_object(data,'$.vin') = '${vin}'
),
-- 计算出初始数据
charge_data as
(
  select
    vin,
    totalCurrent,
    msgTime-lag(msgTime,1,msgTime)  over(partition by vin order by msgTime asc ) as timeDiff  -- 升序排列，获取时间差,第一行取0
  from ods_data
),
-- 计算出充电容量
charge_electricity  as
(
  select
    vin,
    sum(timeDiff/(60*60 ) * totalCurrent)/(${end_soc}-${start_soc})  as chargeCapacity   -- 充电容量
  from  charge_data
  group by vin
),
-- 查询出最初始五次的充电数据并计算出容量平均值
ini_data as
(
  select
    tmp1.vin,
    avg(tmp1.chargeCapacity) as avg_ini_chargeCapacity
  from
      (select
        vin,
        chargeCapacity,
        chargeStart
      from ${db}.battery_attenuation_es
      where vin = '${vin}'
      and chargeStart < '${start_time}'
      order by chargeStart asc
      limit 5) as tmp1
  group by tmp1.vin
),
-- 计算后五次的容量平均值
last_data as
(
  select
    tmp2.vin,
    avg(tmp2.chargeCapacity) as avg_last_chargeCapacity
  from (
      select
        vin,
        chargeCapacity,
        chargeStart
      from  ${db}.battery_attenuation_es
      where vin = '${vin}'
      order by chargeStart desc
      limit 5
  ) as tmp2
  group by vin
),
-- 计算衰减值
attenuation_data as
(
  select
    ini_data.vin,
    concat(cast( abs((last_data.avg_last_chargeCapacity/ini_data.avg_ini_chargeCapacity)*100) as string),'%')  as attenuation_value
  from ini_data join last_data on ini_data.vin = last_data.vin
),
-- 获取车辆的第一次充电数据
first_charge as
(
  select
    vin,
    chargeCapacity,
    chargeStart
  from  ${db}.battery_attenuation_es
  where vin = '${vin}'
  order by chargeStart asc
  limit 1
)
-- 添加衰减值,将充电记录插入到es索引中，并判断是否预警
insert into table ${db}.battery_attenuation_es
select
  ch.vin,
  '${start_time}',
  '${end_time}',
  cast('${odo}' as double),
  ch.chargeCapacity,
  ad.attenuation_value,
  if(ch.chargeCapacity/fc.chargeCapacity * 100 > '${th}','0','1')
from charge_electricity as ch join attenuation_data as ad
on ch.vin = ad.vin
join first_charge as fc
on ch.vin = fc.vin;


-- 下面是找出连续几次发生预警的且满足条件的充电记录
with
seq as   -- 对充电按照时间进行降序编号
(
  select
      vin,
      chargeStart,
      chargeEnd,
      iswarning,
      row_number() over(partition by vin order by chargeStart desc) as rk
  from ${db}.battery_attenuation_es
  where vin = '${vin}'
),
last as  -- 最后一条发生预警的数据
(
  select
    vin,
    chargeStart,
    chargeEnd
  from seq where rk = 2 and iswarning = '1'
  and '0' in (select t.iswarning from seq as t where t.rk = '1' )
),
--  预警发生时的前一条数据
first as
(
  select
    f.vin,
    chargeStart,
    chargeEnd
  from (
        select
          vin,
          chargeStart,
          chargeEnd,
          row_number() over(partition by vin order by chargeStart desc) as rk
        from ${db}.battery_attenuation_es
        where iswarning = '0'
  ) as f
  where f.rk = 2
),
border as
(
  select
  last.vin,
  first.chargeStart as startTime,
  last.chargeEnd as endTime
  from first join last on first.vin = last.vin
  where first.chargeStart <= last.chargeStart
),
-- 预警时间内的充电数据
warning_data as
(
  select
    d.vin,
    d.chargeStart,
    d.chargeEnd
  from
      (
        select
          vin,
          chargeStart,
          chargeEnd
        from  ${db}.battery_attenuation_es
        where vin = '${vin}' and iswarning = '1'
      ) as d
  join border on d.vin = border.vin
  where d.chargeStart > border.startTime and  d.chargeEnd <= border.endTime
),
-- 获取车辆的基础信息
vehicle_base as
(
  select
    vin,
    battery_type
  from ${db}.vehicle_base_info
  where vin  = '${vin}'
)
-- 插入数据
insert into table ${db}.battery_warning_info_es
select
  t.vin,
  c.vehicleType,
  c.enterprise,
  c.licensePlate,
  vehicle_base.battery_type,
  '高风险',
  c.province,
  t.startTime,
  t.endTime,
  '电池包衰减预警',
  '电池包衰减预警',
  '1',
  null,
  null
from  (select vin,min(chargeStart) as startTime,max(chargeEnd) as endTime from warning_data group by vin) as t
join    -- 从充电记录里面获取到省份等信息
  (
    select
      vin,
      enterprise,
      vehicleType,
      province,
      licensePlate,
      chargeEndTime
    from ${db}.charge_record_es where vin = '${vin}'
  ) as c
on t.vin = c.vin and t.endTime = c.chargeEndTime
join vehicle_base on vehicle_base.vin = t.vin

"
hive  -e "${sql}"