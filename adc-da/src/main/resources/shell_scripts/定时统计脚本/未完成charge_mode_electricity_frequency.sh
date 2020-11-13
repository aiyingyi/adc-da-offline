#!/bin/bash

db=warningplatform

# 计算充电方式，电量和频次计算


#  时间戳需要变动

# 充电开始，结束时间
start_time=$1
end_time=$2
vin=$3

# 定义快充和慢充
slowcharge=0
quickcharge=1

# 定义充电状态
charge=1


sql="

with
-- 获取车辆充电时间内的原始数据
preprocess_vehicle_data0 as
(
  select
      get_json_object(data,'$.vin') vin,
      get_json_object(data,'$.province') province,
      get_json_object(data,'$.msgTime') msgTime,
      get_json_object(data,'$.maxCellVoltageNum') maxCellVoltageNum,
      get_json_object(data,'$.minCellVoltageNum') minCellVoltageNum,
      get_json_object(data,'$.vehicleType') vehicleType,
      get_json_object(data,'$.enterprise') enterprise,
      cast(get_json_object(data,'$.totalCurrent') as double)  totalCurrent,
      cast(substring(get_json_object(data,'$.soc'),0,length(get_json_object(data,'$.soc'))-1) as double)/100  soc
  from ${db}.ods_preprocess_vehicle_data where dt>=date_format('${start_time}','yyyy-MM-dd')
  and dt<=date_format('${end_time}','yyyy-MM-dd')      --根据日期分区查找数据
  and get_json_object(data,'$.msgTime') >= '${start_time}'
  and get_json_object(data,'$.msgTime') <= '${end_time}'
  and get_json_object(data,'$.vin') = '${vin}'
-- and get_json_object(data,'$.chargeStatus') = '${charge}'   匹配充电状态？
),
-- 计算出初始数据
preprocess_vehicle_data1 as
(
  select
    enterprise,
    vehicleType,
    province,
    vin,
    maxCellVoltageNum,
    minCellVoltageNum,
    totalCurrent,
    msgTime-lag(msgTime,1,msgTime)  over(partition by vin order by msgTime asc ) as timeDiff,  -- 获取时间差,第一行取0
    min(soc) over(partition by vin) as chargeStartSOC,  -- 开始SOC
    max(soc) over(partition by vin) as chargeEndSOC     -- 结束SOC
  from preprocess_vehicle_data0
),
-- 计算出充电模式以及电量
charge_mode_electricity  as
(
  select
    vin,
    case when avg(totalCurrent)>= 30  or max(totalCurrent) >=70 then '${quickcharge}' else '${slowcharge}' end as  chargeType,
    sum(timeDiff/(1000*60*60.0 ) * totalCurrent) as chargeElectricity
  from  preprocess_vehicle_data1
  group by vin
),
-- 计算最大电压单体频次
max_vol_cell_frequency as
(   select
     '${vin}' as vin,
     ini.CellVoltageNum as maxCellVoltageNum,
     if(max_stat.maxCellVoltageNum is null,0,max_stat.max_total) as frequency
    from
       (select
        vin,
        maxCellVoltageNum,
        count(maxCellVoltageNum) as max_total
       from preprocess_vehicle_data0
       group by vin,maxCellVoltageNum) as max_stat     -- 统计出最大电压单体频次
    right join ${db}.ini_vol_cell_frequency as ini
    on ini.CellVoltageNum = max_stat.maxCellVoltageNum
),
-- 计算最低电压单体频次
min_vol_cell_frequency as
(   select
     '${vin}' as vin,
     ini.CellVoltageNum as minCellVoltageNum,
     if(min_stat.minCellVoltageNum is null,0,min_stat.min_total) as frequency
    from
       (select
        vin,
        minCellVoltageNum,
        count(minCellVoltageNum) as min_total
       from preprocess_vehicle_data0
       group by vin,minCellVoltageNum) as min_stat     -- 统计出最大电压单体频次
    right join ${db}.ini_vol_cell_frequency as ini
    on ini.CellVoltageNum = min_stat.minCellVoltageNum
),
-- 统计出最大最低电压单体频次，并放到数组里面
cell_vol_frequency as
(
  select
    max_fre.vin as vin,
    collect_list(cast (max_fre.frequency as bigint)) as max_frequency,
    collect_list(cast (min_fre.frequency as bigint)) as min_frequency
  from max_vol_cell_frequency as max_fre
  join min_vol_cell_frequency as min_fre
  on max_fre.maxCellVoltageNum = min_fre.minCellVoltageNum
  group by max_fre.vin
)

insert into table ${db}.charge_record_es
select
    data1.enterprise,
    data1.vehicleType,
    data1.province,
    data1.vin,
    base.licensePlate,
    date_format('${start_time}','yyyy-MM-dd HH:mm:ss'),
    date_format('${end_time},'yyyy-MM-dd HH:mm:ss'),
    data1.chargeStartSOC,
    data1.chargeEndSOC,
    round(cme.chargeElectricity,1),  -- 保留一位小数
    cme.chargeType,
    fre.max_frequency,
    fre.min_frequency
from (select * from preprocess_vehicle_data1 limit 1) as data1
join charge_mode_electricity  as cme
on data1.vin = cme.vin
join cell_vol_frequency as fre
on data1.vin = fre.vin
join ${db}.vehicle_base_info as base
on data1.vin = base.vin;

"
hive -e  "${sql}"








