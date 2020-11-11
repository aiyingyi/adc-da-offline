#!/bin/bash

db=warningplatform

# 获取昨天以及前天的日期，充电时间以结束时间为准,每天执行一次



yesterday=`date -d "1 day ago" "+%Y-%m-%d"`
before_yesterday=`date -d "2 day ago" "+%Y-%m-%d"`


if [[ -n "$1" ]]; then
    yesterday="$1"
    before_yesterday=`date -d "1 day ago ${yesterday}" "+%Y-%m-%d"`
fi

# 定义充电状态
uncharge=0
charge=1

sql="
with
-- 获取昨天的充电记录
charge_record as
(
  select
      province,
      vehicleType,
      vin,
      chargeStartTime,
      chargeEndTime,
      chargeType
  from ${db}.charge_record_es where date_format(chargeEndTime,'yyyy-MM-dd') = '${yesterday}'
),
charge_data as
(
    select
        data0.province,
        data0.vehicleType,
        data0.vin,
        cast (data0.totalCurrent / 5 as int) as cur_rk,
        cr.chargeType
    from
        (
          select  -- 获取昨天和前天的预处理的充电状态数据
            pre_data.province,
            pre_data.vehicleType,
            pre_data.vin,
            pre_data.msgTime,
            pre_data.totalCurrent
          from  ${db}.dwd_preprocess_vehicle_data as pre_data
          where pre_data.dt = '${yesterday}' or  pre_data.dt = '${before_yesterday}'
          and pre_data.chargeStatus = '${charge}'
        ) as data0
    join charge_record as  cr          -- 和充电记录join，匹配到对应的充电方式
    on data0.province = cr.province
    and data0.vin = cr.vin
    where date_format(data0.msgTime,'yyyy-MM-dd HH:mm:ss') >= cr.chargeStartTime
    and   date_format(data0.msgTime,'yyyy-MM-dd HH:mm:ss') <= cr.chargeEndTime
),
-- 按照省份车型，统计每辆车在不同充电方式下的电流频率
ods_fre  as
(
  select
    province,
    vehicleType,
    vin,
    chargeType,
    cur_rk,
    count(cur_rk) as frequency
  from charge_data
  group by province,vehicleType,vin,chargeType,cur_rk
),
-- 获取省份和车型,vin,chargeType维度表和初始电流频次表的笛卡尔积
ini_fre as
(
  select
    tmp1.province,
    tmp1.vehicleType,
    tmp1.vin,
    tmp1.chargeType,
    tmp2.current_rk,
    tmp2.frequency
  from
    (
      select    -- 按照省份和车型，vin和充电方式进行去重，获取维度
        province,
        vehicleType,
        vin,
        chargeType
      from charge_data
      group by province,vehicleType,vin,chargeType
    ) as tmp1
  join ${db}.ini_current_frequency  as tmp2
)

insert into table ${db}.charge_current_frequency_es
select
  sta.province,
  sta.vehicleType,
  sta.vin,
  sta.chargeType,
  collect_list(cast (frequency as bigint)),
  '${yesterday}'
from
  (
    select     -- 注意按照current_rk进行排序时候一定要将current_rk转换成int类型，否则按照字典顺序进行排序
      ini_fre.province,
      ini_fre.vehicleType,
      ini_fre.vin,
      ini_fre.chargeType,
      ini_fre.current_rk,
      nvl(ods_fre.frequency,ini_fre.frequency) as frequency
    from ini_fre left join ods_fre
    on  ini_fre.province = ods_fre.province
    and ini_fre.vehicleType = ods_fre.vehicleType
    and ini_fre.vin = ods_fre.vin
    and ini_fre.chargeType = ods_fre.chargeType
    and ini_fre.current_rk = ods_fre.cur_rk
    order by ini_fre.province,ini_fre.vehicleType,ini_fre.vin,ini_fre.chargeType,cast (ini_fre.current_rk as  int)
  ) as sta
group by sta.province,sta.vehicleType,sta.vin,sta.chargeType
"

hive -e  "${sql}"

