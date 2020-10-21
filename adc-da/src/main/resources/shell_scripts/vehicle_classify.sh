#!/bin/bash

# 车辆分类脚本，每月执行一次，每月最后一天执行，数据统计范围为上个月最后一天到本月最后一天之前
# 计算之前  1. 导入前一天的数据到dwd层 2.sqoop更新一下车辆的基础信息表

db=warningplatform

# 获取当前日期,当前日期应该是本月最后一天
do_date=`date  "+%Y-%m-%d %H:%M:%S"`
last_month=`date  -d "${do_date} 1 month ago" "+%Y-%m"`
first_day_this_month=`date "+%Y-%m-01"`
# 获取上个月最后一天的日期
start_time=`date -d "${first_day_this_month}  last day"   "+%Y-%m-%d"`


# 1. 缺少sqoop脚本，更新基础信息表中的数据
# 2. 计算车辆的最初使用季度
sql="
with
--  获取这个月的车辆数据，根据vin分区，按照行车里程进行排序
vehicle_rk as
(
    select
        enterprise,
        vin,
        cast(date_format(msgTime,'MM') as int)  month,
        odo,
        row_number() over(partition by enterprise,vin order by odo desc)  rk
    from ${db}.dwd_preprocess_vehicle_data
    where dt>='${start_time}' and dt < '${do_date}'
),
-- 计算每一辆车的初始使用季度
vehicle_ini as
(
    select
        enterprise,
        vin,
        case
        when month>=3 and month <=5 then 'spring'
        when month>=6 and month <=8 then 'summer'
        when month>=9 and month <=11 then 'automn'
        else 'winter' end  as quarter
    from vehicle_rk   where rk = 1 and vehicle_rk.odo <= 1
    union all
    select
        enterprise,
        vin,
        'none' as quarter
    from vehicle_rk   where rk = 1 and vehicle_rk.odo > 1
)

--  更新车辆最初使用时间表
insert overwrite table ${db}.vehicle_initial
select
    if(old.vin is null,new.enterprise,old.enterprise),
    if(old.vin is null,new.vin,old.vin),
    if(old.vin is null,new.quarter,old.quarter)
from ${db}.vehicle_initial  old full outer join vehicle_ini  new
on  old.vin = new.vin;

--  获取本月中每辆车的最后一条数据，先按照时间排名
with
vehicle_last_data as
(
    select
      vehicle_data_rk.enterprise,
      vehicle_data_rk.vin,
      vehicle_data_rk.province,
      vehicle_data_rk.vehicleType,
      cast(vehicle_data_rk.odo/10000 as int) as odo_level
    from
      (
          select
              enterprise,
              vin,
              province,
              vehicleType,
              odo,
              msgTime,
              row_number() over(partition by enterprise,vin order by msgTime desc) as rk
          from ${db}.dwd_preprocess_vehicle_data
          where dt>='${start_time}' and dt < '${do_date}'
      ) as vehicle_data_rk
    where vehicle_data_rk.rk = 1
)
-- 计算当月的新分类，注意：本月没有的车辆按照上个月的进行分类
insert into table  ${db}.vehicle_classification_es
select
    last.enterprise,
    last.vin,
    concat_ws('-',last.province,last.vehicleType,date_format(base.delivery_time,'yyyy'),ini.quarter,cast(last.odo_level as string)) as classification,
    date_format('${do_date}','yyyy-MM') as dt
from  vehicle_last_data  last join  ${db}.vehicle_initial as ini
on last.vin = ini.vin
join  ${db}.vehicle_base_info  base
on last.vin = base.vin
"
hive -e  "${sql}"








