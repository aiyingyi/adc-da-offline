#!/bin/bash

db=warningplatform

# 充电压差扩大模型算法，缺少将soc由字符串例如80%转换成小数步骤
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
    get_json_object(data,'$.differenceCellVoltage') as differenceCellVoltage,
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
charge_info as
(
  select
    vin,
    cast(charge_start_time as bigint) charge_start_time,
    avg(differenceCellVoltage) as avg_vol_diff
  from  charge_data group by vin,charge_start_time
),
charge_statistics as
(
  select
      base.vin,
      '${args[$[2*$window_size-2]]}' as  last_charge_time,
      collect_list(avg_vol_diff) as vol_diff,
      collect_list( cast (day_diff as double)/(1000*3600*24)) as day_diff
  from
      (select    --计算充电之间的时间差
        vin,
        avg_vol_diff,
        charge_start_time-LAG(charge_start_time,1,charge_start_time)  over(partition by vin order by charge_start_time asc)  as day_diff
      from charge_info) as base
  group by base.vin
)

-- 将压差数组和时差数组输入自定义函数，输出是否预警，并写入记录

insert into table ${db}.charge_vol_day_diff_es
select
  vin,
  date_format(last_charge_time,'yyyy-MM-dd HH:mm:ss'),  --最近10次充电中最后一次充电的开始时间
  vol_diff,
  day_diff,
  ${db}.charge_vol_diff_exp(vol_diff,day_diff)
from charge_statistics
"

hive  -e "${sql}"