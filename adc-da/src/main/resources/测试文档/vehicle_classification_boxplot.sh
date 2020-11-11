#!/bin/bash

# 五种预警箱线图统计脚本，每天执行一次，
# named_struct 会把整数直接当成int类型，从而不能插入double类型中

db=warningplatform

coefficient=1.5

# 获取当前日期

do_date=`date -d "1 day ago" "+%Y-%m-%d %H:%M:%S"`


if [[ -n "$1" ]]; then
    do_date=`date -d "1 day ago ${1}" "+%Y-%m-%d %H:%M:%S"`
fi

# 计算之前应该导入前一天的数据到dwd_preprocess_vehicle_data中
# 首先执行 ods_to_dwd_preprocess.sh 脚本，将前一天的数据导入到dwd层
sql="
with
-- 计算每辆车的平均值,并添加分类字段
avg_vehicle_data_perday as
(
  select
    avg_data.vin,
    c.classification,
    avg_data.diff_Voltage,
    avg_data.diff_temper,
    avg_data.temper_rate,
    avg_data.temper,
    avg_data.resistance
  from
  (
    select
      vin,
      round(avg(differenceCellVoltage),1)  diff_Voltage,
      round(avg(maxProbeTemperature-minProbeTemperature),1) diff_temper,
      round(avg(maxTemperatureRate),1) temper_rate,
      round(avg(averageProbeTemperature),1) temper,
      round(avg(resistance),1) resistance
    from ${db}.dwd_preprocess_vehicle_data
    where dt = date_format('${do_date}','yyyy-MM-dd')
    group by vin
  )  as avg_data
  join (
    select
      vin,
      classification
    from ${db}.vehicle_classification_es
    where dt = date_format('${do_date}','yyyy-MM')
  ) as c
  on c.vin = avg_data.vin
),
 -- 根据车类，异常类型去计算分位数
 -- 计算压差的分位数
diff_Voltage_info as
(
    select
        boxplot.classification,
        boxplot.q3,
        boxplot.q2,
        boxplot.q1,
        (boxplot.q3 + boxplot.q1*${coefficient}) as max_value,
        (boxplot.q3 - boxplot.q1*${coefficient}) as min_value
    from
        (
          select
              ap.classification,
              percentile_approx(diff_Voltage, 0.75) as q3,
              percentile_approx(diff_Voltage, 0.5) as q2,
              percentile_approx(diff_Voltage, 0.25) as q1
          from avg_vehicle_data_perday as ap
          group by ap.classification
        ) as boxplot
),
temper_rate_info as
(
    select
        boxplot.classification,
        boxplot.q3,
        boxplot.q2,
        boxplot.q1,
        (boxplot.q3 + boxplot.q1*${coefficient}) as max_value,
        (boxplot.q3 - boxplot.q1*${coefficient}) as min_value
    from
        (
          select
              ap.classification,
              percentile_approx(temper_rate, 0.75) as q3,
              percentile_approx(temper_rate, 0.5) as q2,
              percentile_approx(temper_rate, 0.25) as q1
          from avg_vehicle_data_perday as ap
          group by ap.classification
        ) as boxplot
),
temper_info as
(
    select
        boxplot.classification,
        boxplot.q3,
        boxplot.q2,
        boxplot.q1,
        (boxplot.q3 + boxplot.q1*${coefficient}) as max_value,
        (boxplot.q3 - boxplot.q1*${coefficient}) as min_value
    from
        (
          select
              ap.classification,
              percentile_approx(temper, 0.75) as q3,
              percentile_approx(temper, 0.5) as q2,
              percentile_approx(temper, 0.25) as q1
          from avg_vehicle_data_perday as ap
          group by ap.classification
        ) as boxplot
),
diff_temper_info as
(
    select
        boxplot.classification,
        boxplot.q3,
        boxplot.q2,
        boxplot.q1,
        (boxplot.q3 + boxplot.q1*${coefficient}) as max_value,
        (boxplot.q3 - boxplot.q1*${coefficient}) as min_value
    from
        (
          select
              ap.classification,
              percentile_approx(diff_temper, 0.75) as q3,
              percentile_approx(diff_temper, 0.5) as q2,
              percentile_approx(diff_temper, 0.25) as q1
          from avg_vehicle_data_perday as ap
          group by ap.classification
        ) as boxplot
),
resistance_info as
(
     select
        boxplot.classification,
        boxplot.q3,
        boxplot.q2,
        boxplot.q1,
        (boxplot.q3 + boxplot.q1*${coefficient}) as max_value,
        (boxplot.q3 - boxplot.q1*${coefficient}) as min_value
    from
        (
          select
              ap.classification,
              percentile_approx(resistance, 0.75) as q3,
              percentile_approx(resistance, 0.5) as q2,
              percentile_approx(resistance, 0.25) as q1
          from avg_vehicle_data_perday as ap
          group by ap.classification
        ) as boxplot
)
insert into table ${db}.warning_boxplot_perday_es
select
    dvs.classification,
    named_struct('Q3',dvs.q3,'Q2',dvs.q2,'Q1',dvs.q1,'maxvalue',dvs.max_value,'minvalue',dvs.min_value) as vol_diff_exception,
    named_struct('Q3',trs.q3,'Q2',trs.q2,'Q1',trs.q1,'maxvalue',trs.max_value,'minvalue',trs.min_value) as temper_rate_exception,
    named_struct('Q3',ts.q3,'Q2',ts.q2,'Q1',ts.q1,'maxvalue',ts.max_value,'minvalue',ts.min_value) as temper_exception,
    named_struct('Q3',dts.q3,'Q2',dts.q2,'Q1',dts.q1,'maxvalue',dts.max_value,'minvalue',dts.min_value) as temper_diff_exception,
    named_struct('Q3',rs.q3,'Q2',rs.q2,'Q1',rs.q1,'maxvalue',rs.max_value,'minvalue',rs.min_value) as resistance_exception,
    date_format('${do_date}','yyyy-MM-dd') as dt
from  diff_Voltage_info   dvs
join  temper_rate_info    trs on  dvs.classification = trs.classification
join  temper_info         ts  on  dvs.classification = ts.classification
join  diff_temper_info    dts on  dvs.classification = dts.classification
join  resistance_info     rs  on  dvs.classification = rs.classification
"
hive -e  "${sql}"





