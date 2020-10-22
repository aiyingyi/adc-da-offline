#!/bin/bash

db=warningplatform

# 电压单位问题
# 指定算法计算的窗口大小(充电次数)
window_size=10

# 将脚本参数放入到数组里面，包含20个时间戳以及vin码
_index=0
for i in "$@"
do
  args[_index]=$i
  let _index++
done

vin=${args[$[${#args[@]}-1]]}

query_sql="
select
    get_json_object(data,'$.vin') as vin,
    cast(get_json_object(data,'$.differenceCellVoltage') as double)  as differenceCellVoltage,
"
# 拼接每次充电查询的sql
for((i = 1;i<=$[${window_size}];i++))
do
   charge_data_sql=${charge_data_sql}${query_sql}"'${args[$[2*$i-2]]}' as charge_start_time  from ${db}.ods_preprocess_vehicle_data "" where dt>= date_format('${args[$[2*$i-2]]}','yyyy-MM-dd')
               and dt <= date_format('${args[$[2*$i-1]]}','yyyy-MM-dd')
               and get_json_object(data,'$.msgTime') >= '${args[$[2*$i-2]]}'
               and get_json_object(data,'$.msgTime') <= '${args[$[2*$i-1]]}'
               and get_json_object(data,'$.vin') = '${vin}'
               and cast (substring(get_json_object(data,'$.soc'),0,length(get_json_object(data,'$.soc'))-1) as double)/100 >= 0.8
             "
   if [ $i -lt $[$window_size] ]
   then
      charge_data_sql=${charge_data_sql}"union all"
   fi
done

sql="
with
charge_data as   -- 最近10次有效充电为且充电数据帧soc > 0.8 的数据
(
 ${charge_data_sql}
),
-- 计算出最后一次充电的省份信息
charge_province as
(
  select
    get_json_object(data,'$.vin') as vin,
    get_json_object(data,'$.province') as province
  from ${db}.ods_preprocess_vehicle_data
  where dt = date_format('${args[$[2*$window_size-1]]}','yyyy-MM-dd')
  and msgTime = '${args[$[2*$window_size-1]]}'
),

-- 计算平均压差，并按照充电时间排序
charge_info as
(
  select
    vin,
    charge_start_time,
    avg(differenceCellVoltage * 1000) as avg_vol_diff    -- 将压差转换成mv
  from  charge_data
  group by vin,charge_start_time
  order by vin,charge_start_time asc
),
-- 每次充电和第一次充电的时间差
charge_time as
(
  select
    tmp.vin as vin,
    collect_list(tmp.timeDiff) as timeDiff
  from
    (
      select
        vin,
        cast ((cast(charge_start_time as bigint)-cast('${1}' as bigint))/(1000*60*60*24) as int) as timeDiff
      from charge_info
    ) as tmp
  group by tmp.vin
),
charge_statistics as
(
  select
    t1.vin,
    t1.vol_diff as vol_diff,   -- 压差
    t1.charge_start_time as charge_start_time, -- 充电开始时间
    t2.timeDiff as timeDiff    --  时间差
  from
    (select
        vin,
        collect_list(avg_vol_diff) as vol_diff,
        collect_list(charge_start_time) as charge_start_time
    from charge_info
    group by vin) as t1
  join charge_time as t2
  on t1.vin = t2.vin
)
--  前10次充电并且发生预警的计算结果
insert into table ${db}.charge_vol_diff_exp
select
  tmp.vin,
  tmp.startTime,
  tmp.endTime,
  tmp.vol_diff,
  tmp.timeDiff
from
  (select
    vin,
    date_format('${args[$[2*$window_size-2]]}', 'yyyy-MM-dd HH:mm:ss') as startTime,  -- 最近10次充电中最后一次充电的开始时间
    date_format('${args[$[2*$window_size-1]]}', 'yyyy-MM-dd HH:mm:ss') as endTime,    -- 最近10次充电中最后一次充电的结束时间
    vol_diff,  -- 压差数组
    timeDiff,  -- 时间间隔
    ${db}.charge_vol_diff_exp(vol_diff,charge_start_time) as iswarning
  from charge_statistics)  tmp
where tmp.iswarning = '1';

-- 写入预警信息表
insert into table ${db}.battery_warning_info_es
select
  a.vin,
  b.vehicleType,
  b.enterprise,
  b.licensePlate,
  b.batteryType
  '1',         --  预警等级为1
  p.vin,
  w.startTime,
  w.endTime,
  '充电压差扩大',
  '充电压差扩大',
  null,
  null,
  null
from (select vin,startTime,endTime,vol_diff,timeDiff from ${db}.charge_vol_diff_exp where startTime = date_format('${args[$[2*$window_size-2]]}', 'yyyy-MM-dd HH:mm:ss')) as w
join ${db}.vehicle_base_info as b
on w.vin = b.vin
join charge_province p on w.vin = p.vin;


-- 将拟合直线写入es表格
insert into table ${db}.charge_vol_diff_exp_es
select
  vin,
  startTime,
  endTime,
  vol_diff,
  timeDiff
from ${db}.charge_vol_diff_exp
where startTime = date_format('${args[$[2*$window_size-2]]}', 'yyyy-MM-dd HH:mm:ss');


"

hive  -e "${sql}"

