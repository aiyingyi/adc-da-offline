#!/bin/bash

# 1. 执行之前，执行数据导入脚本，将前一天的数据导入到dwd层

db=warningplatform

coefficient=1.5


# 获取上周日期
startDate=`date  -d "7 days  ago" "+%Y-%m-%d %H:%M:%S"`
endDate=`date  -d "1 days  ago" "+%Y-%m-%d %H:%M:%S"`


# 获取上周周末的上个月

sql="
with
vehicle_data as
(
  select
      enterprise,
      province,
      vehicleType,
      vin,
      differenceCellVoltage,
      totalVoltage,



      chargeStatus,
      differenceCellVoltage,
      maxTemperatureRate,
      maxProbeTemperature,
      minProbeTemperature,
      resistance
  from ${db}.dwd_preprocess_vehicle_data
  where dt >= date_format('${startDate}','yyyy-MM-dd')
  and   dt <= date_format('${endDate}','yyyy-MM-dd')
  group by enterprise,province
),
avg_data_perweek as     -- 计算每周车辆在不同工况下的平均值
(
    select
      enterprise,
      vin,
      classification,
      chargeStatus,
      round(avg(differenceCellVoltage),1)  diff_voltage,
      round(avg(maxTemperatureRate),1) temper_rate,
      round(avg(maxProbeTemperature),1) temper,
      round(avg(maxProbeTemperature-minProbeTemperature),1) diff_temper,
      round(avg(resistance),1) resistance
    from vehicle_data
    group by  enterprise,vin,classification,chargeStatus
),
particle_location as    -- 计算不同企业、车类、不同工况的分位位置
(
    select
      enterprise,
      classification,
      chargeStatus,
      cast(3*(count(vin) + 1) / 4 as int) as q3_int_part,
      (count(vin)+1)*3 / 4 % 1            as q3_float_part,
      cast((count(vin) + 1) / 4 as int)   as q1_int_part,
      (count(vin)+1) / 4 % 1              as q1_float_part
    from  avg_data_perweek
    group by  enterprise,classification,chargeStatus
),
avg_data_rank  as       -- 计算平均数据的不同车类不同工况的各个维度的排名
(
    select
        enterprise,
        classification,
        chargeStatus,
        diff_voltage,
        lead(diff_voltage,1, 0) over(partition by enterprise,classification,chargeStatus order by diff_voltage)   next_diff_vol,
        row_number() over(partition by enterprise,classification,chargeStatus order by diff_voltage)   vol_rk,
        temper_rate,
        lead(temper_rate,1, 0) over(partition by enterprise,classification,chargeStatus order by temper_rate)   next_temper_rate,
        row_number() over(partition by enterprise,classification,chargeStatus order by temper_rate)   temper_rate_rk,
        temper,
        lead(temper,1, 0) over(partition by enterprise,classification,chargeStatus order by temper)   next_temper,
        row_number() over(partition by enterprise,classification,chargeStatus order by temper)   temper_rk,
        diff_temper,
        lead(diff_temper,1, 0) over(partition by enterprise,classification,chargeStatus order by diff_temper)   next_diff_temper,
        row_number() over(partition by enterprise,classification,chargeStatus order by diff_temper)   diff_temper_rk,
        resistance,
        lead(resistance,1, 0) over(partition by enterprise,classification,chargeStatus order by resistance)   next_resistance,
        row_number() over(partition by enterprise,classification,chargeStatus order by resistance)   resistance_rk
    from avg_data_perweek
),
diff_voltage_info as      -- 按企业，类别，工况去计算压差的箱线指标
(
    select
        q3.enterprise,
        q3.classification,
        q3.chargeStatus,
        (q3.vol_q3 + q1.vol_q1* ${coefficient}) as vol_max_value
    from
      (select
        data_rk.enterprise,
        data_rk.classification,
        data_rk.chargeStatus,
        data_rk.diff_voltage * (1-lo.q3_float_part) + data_rk.next_diff_vol* lo.q3_float_part  as vol_q3
      from avg_data_rank  data_rk  join particle_location lo
      on  data_rk.enterprise = lo.enterprise
      and  data_rk.classification = lo.classification
      and data_rk.chargeStatus = lo.chargeStatus
      and data_rk.vol_rk = lo.q3_int_part
      )  as q3
    join
      (select
        data_rk.enterprise,
        data_rk.classification,
        data_rk.chargeStatus,
        data_rk.diff_voltage * (1-lo.q1_float_part) + data_rk.next_diff_vol* lo.q1_float_part  as vol_q1
      from avg_data_rank  data_rk  join particle_location lo
      on  data_rk.enterprise = lo.enterprise
      and  data_rk.classification = lo.classification
      and data_rk.chargeStatus = lo.chargeStatus
      and data_rk.vol_rk = lo.q1_int_part
      )  as q1
    on q3.enterprise = q1.enterprise
    and q3.classification = q1.classification
    and q3.chargeStatus = q1.chargeStatus
),
temper_rate_info as      -- 按类别，工况去计算temper_rate的箱线指标
(
    select
        q3.enterprise,
        q3.classification,
        q3.chargeStatus,
        (q3.temper_rate_q3 + q1.temper_rate_q1* ${coefficient}) as  temper_rate_max_value
    from
      (select
        data_rk.enterprise,
        data_rk.classification,
        data_rk.chargeStatus,
        data_rk.temper_rate * (1-lo.q3_float_part) + data_rk.next_temper_rate* lo.q3_float_part  as  temper_rate_q3
      from avg_data_rank  data_rk  join particle_location lo
      on  data_rk.enterprise = lo.enterprise
      and  data_rk.classification = lo.classification
      and data_rk.chargeStatus = lo.chargeStatus
      and data_rk.temper_rate_rk = lo.q3_int_part
      )  as q3
    join
      (select
        data_rk.enterprise,
        data_rk.classification,
        data_rk.chargeStatus,
        data_rk.temper_rate * (1-lo.q1_float_part) + data_rk.next_temper_rate * lo.q1_float_part  as temper_rate_q1
      from avg_data_rank  data_rk  join particle_location lo
      on  data_rk.enterprise = lo.enterprise
      and  data_rk.classification = lo.classification
      and data_rk.chargeStatus = lo.chargeStatus
      and data_rk.temper_rate_rk = lo.q1_int_part
      )  as q1
    on q3.enterprise = q1.enterprise
    and q3.classification = q1.classification
    and q3.chargeStatus = q1.chargeStatus
),
temper_info as      -- 按类别，工况去计算temper的箱线指标
(
    select
        q3.enterprise,
        q3.classification,
        q3.chargeStatus,
        (q3.temper_q3 + q1.temper_q1* ${coefficient}) as  temper_max_value
    from
      (select
        data_rk.enterprise,
        data_rk.classification,
        data_rk.chargeStatus,
        data_rk.temper * (1-lo.q3_float_part) + data_rk.next_temper * lo.q3_float_part  as  temper_q3
      from avg_data_rank  data_rk  join particle_location lo
      on  data_rk.enterprise = lo.enterprise
      and  data_rk.classification = lo.classification
      and data_rk.chargeStatus = lo.chargeStatus
      and data_rk.temper_rk = lo.q3_int_part
      )  as q3
    join
      (select
        data_rk.enterprise,
        data_rk.classification,
        data_rk.chargeStatus,
        data_rk.temper * (1-lo.q1_float_part) + data_rk.next_temper* lo.q1_float_part  as  temper_q1
      from avg_data_rank  data_rk  join particle_location lo
      on  data_rk.enterprise = lo.enterprise
      and  data_rk.classification = lo.classification
      and data_rk.chargeStatus = lo.chargeStatus
      and data_rk.temper_rk = lo.q1_int_part
      )  as q1
    on q3.enterprise = q1.enterprise
    and q3.classification = q1.classification
    and q3.chargeStatus = q1.chargeStatus
),
diff_temper_info as      -- 按类别，工况去计算diff_temper的箱线指标
(
    select
        q3.enterprise,
        q3.classification,
        q3.chargeStatus,
        (q3.diff_temper_q3 + q1.diff_temper_q1* ${coefficient}) as  diff_temper_max_value
    from
      (select
        data_rk.enterprise,
        data_rk.classification,
        data_rk.chargeStatus,
        data_rk.diff_temper * (1-lo.q3_float_part) + data_rk.next_diff_temper* lo.q3_float_part  as  diff_temper_q3
      from avg_data_rank  data_rk  join particle_location lo
      on  data_rk.enterprise = lo.enterprise
      and data_rk.classification = lo.classification
      and data_rk.chargeStatus = lo.chargeStatus
      and data_rk.diff_temper_rk = lo.q3_int_part
      )  as q3
    join
      (select
        data_rk.enterprise,
        data_rk.classification,
        data_rk.chargeStatus,
        data_rk.diff_temper * (1-lo.q1_float_part) + data_rk.next_diff_temper* lo.q1_float_part  as  diff_temper_q1
      from avg_data_rank  data_rk  join particle_location lo
      on  data_rk.enterprise = lo.enterprise
      and data_rk.classification = lo.classification
      and data_rk.chargeStatus = lo.chargeStatus
      and data_rk.diff_temper_rk = lo.q1_int_part
      )  as q1
    on q3.enterprise = q1.enterprise
    and q3.classification = q1.classification
    and q3.chargeStatus = q1.chargeStatus
),
resistance_info as      -- 按类别，工况去计算resistance的箱线指标
(
    select
        q3.enterprise,
        q3.classification,
        q3.chargeStatus,
        (q3.resistance_q3 - q1.resistance_q1* ${coefficient}) as  resistance_min_value
    from
      (select
        data_rk.enterprise,
        data_rk.classification,
        data_rk.chargeStatus,
        data_rk.resistance * (1-lo.q3_float_part) + data_rk.next_resistance * lo.q3_float_part  as  resistance_q3
      from avg_data_rank  data_rk  join particle_location lo
      on  data_rk.enterprise = lo.enterprise
      and data_rk.classification = lo.classification
      and data_rk.chargeStatus = lo.chargeStatus
      and data_rk.resistance_rk = lo.q3_int_part
      )  as q3
    join
      (select
        data_rk.enterprise,
        data_rk.classification,
        data_rk.chargeStatus,
        data_rk.resistance * (1-lo.q1_float_part) + data_rk.next_resistance * lo.q1_float_part  as  resistance_q1
      from avg_data_rank  data_rk  join particle_location lo
      on  data_rk.enterprise = lo.enterprise
      and data_rk.classification = lo.classification
      and data_rk.chargeStatus = lo.chargeStatus
      and data_rk.resistance_rk = lo.q1_int_part
    ) as q1
    on q3.enterprise = q1.enterprise
    and q3.classification = q1.classification
    and q3.chargeStatus = q1.chargeStatus
),
mixed_box_plot as       --  按照类别，工况将所有维度的数据连接在一起
(
    select
        vo.enterprise,
        vo.classification,
        vo.chargeStatus,
        vo.vol_max_value,
        rate.temper_rate_max_value,
        temp.temper_max_value,
        diff.diff_temper_max_value,
        res.resistance_min_value
    from  diff_voltage_info  vo join  temper_rate_info rate
    on vo.enterprise = rate.enterprise
    and vo.classification = rate.classification
    and vo.chargeStatus = rate.chargeStatus
    join temper_info  temp
    on vo.enterprise = temp.enterprise
    and vo.classification = temp.classification
    and vo.chargeStatus = temp.chargeStatus
    join diff_temper_info  diff
    on vo.enterprise = diff.enterprise
    and vo.classification = diff.classification
    and vo.chargeStatus = diff.chargeStatus
    join resistance_info  res
    on vo.enterprise = res.enterprise
    and vo.classification = res.classification
    and vo.chargeStatus = res.chargeStatus
),
box_plot as      -- 将同一类型的不同工况组合到一条记录中
(
    select
        charge.enterprise,
        charge.classification,
        charge.vol_max_value as ch_vol,
        uncharge.vol_max_value as unch_vol,
        charge.temper_rate_max_value  as ch_temper_rate,
        uncharge.temper_rate_max_value  as unch_temper_rate,
        charge.temper_max_value  as ch_temper,
        uncharge.temper_max_value  as unch_temper,
        charge.diff_temper_max_value  as ch_diff_temper,
        uncharge.diff_temper_max_value  as unch_diff_temper,
        charge.resistance_min_value  as ch_resistance,
        uncharge.resistance_min_value  as unch_resistance
    from    (select
                enterprise,
                classification,
                vol_max_value,
                temper_rate_max_value,
                temper_max_value,
                diff_temper_max_value,
                resistance_min_value
            from  mixed_box_plot where chargeStatus = '${charge}') as charge
    join    (select
                enterprise,
                classification,
                vol_max_value,
                temper_rate_max_value,
                temper_max_value,
                diff_temper_max_value,
                resistance_min_value
            from  mixed_box_plot where chargeStatus = '${uncharge}') as uncharge
    on  charge.enterprise = uncharge.enterprise
    and  charge.classification = uncharge.classification
)

-- 将每辆车对应类型的各个维度的箱线指标写入es
insert into table ${db}.warning_boxplot_es
select
  c.enterprise,
  c.vin,
  round(b.ch_vol,1) ,
  round(b.unch_vol,1),
  round(b.ch_temper_rate,1),
  round(b.unch_temper_rate,1),
  round(b.ch_temper,1),
  round(b.unch_temper,1),
  round(b.ch_diff_temper,1),
  round(b.unch_diff_temper,1),
  round(b.ch_resistance,1),
  round(b.unch_resistance,1),
  date_format(date_add(next_day('${do_date}','MO'),-14),'yyyy-MM-dd')
from
    (select enterprise,vin,classification from  ${db}.vehicle_classification
    where dt = '${last_month}' ) as c
left  join  box_plot as b
on c.enterprise = b.enterprise
and c.classification = b.classification

"
hive -e  "${sql}"
