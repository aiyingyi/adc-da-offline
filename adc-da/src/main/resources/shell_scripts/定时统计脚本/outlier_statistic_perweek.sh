#!/bin/bash


# 电池包数据离群统计

# 1. 执行之前，执行数据导入脚本，将前一天的数据导入到dwd层
db=warningplatform
coefficient=1.5

# 获取上周日期
startDate=`date  -d "7 days  ago" "+%Y-%m-%d %H:%M:%S"`
endDate=`date  -d "1 days  ago" "+%Y-%m-%d %H:%M:%S"`

if [[ -n "$1" ]]; then
    startDate=`date  -d "7 days  ago ${1}" "+%Y-%m-%d %H:%M:%S"`
    endDate=`date  -d "1 days  ago ${1}" "+%Y-%m-%d %H:%M:%S"`
fi


sql="
with
vehicle_data as
(
  select
      enterprise,
      province,
      vin,
      avg(differenceCellVoltage) as volDiff,
      avg(totalVoltage) as totalVol,
      avg(averageProbeTemperature) as temp,
      avg(differenceTemperature) as tempDiff,
      avg(resistance) as resistance,
      avg(totalCurrent) as totalCurrent,
      avg(maxTemperatureRate) as maxTemperatureRate
  from ${db}.dwd_preprocess_vehicle_data
  where dt >= date_format('${startDate}','yyyy-MM-dd')
  and   dt <= date_format('${endDate}','yyyy-MM-dd')
  group by enterprise,province,vin
),
-- 计算压差的分位数
volDiff_info as
(
    select
        boxplot.enterprise,
        boxplot.province,
        (boxplot.q3 + boxplot.q1*${coefficient}) as max_value,
        (boxplot.q3 - boxplot.q1*${coefficient}) as min_value
    from
        (
          select
              enterprise,
              province,
              percentile_approx(volDiff, 0.75) as q3,
              percentile_approx(volDiff, 0.25) as q1
          from vehicle_data
          group by enterprise,province
        ) as boxplot
),
volDiff_count as
(
  select
    tmp.enterprise,
    tmp.province,
    count(tmp.volDiff) as total
  from
      (
        select
          vehicle_data.enterprise,
          vehicle_data.province,
          vehicle_data.volDiff,
          volDiff_info.max_value,
          volDiff_info.min_value
        from vehicle_data
        join volDiff_info on vehicle_data.enterprise = volDiff_info.enterprise
        and  vehicle_data.province = volDiff_info.province
      ) as tmp
  where tmp.volDiff > tmp.max_value or tmp.volDiff < tmp.min_value
  group by tmp.enterprise,tmp.province
),

-- 计算总电压
totalVol_info as
(
    select
        boxplot.enterprise,
        boxplot.province,
        (boxplot.q3 + boxplot.q1*${coefficient}) as max_value,
        (boxplot.q3 - boxplot.q1*${coefficient}) as min_value
    from
        (
          select
              enterprise,
              province,
              percentile_approx(totalVol, 0.75) as q3,
              percentile_approx(totalVol, 0.25) as q1
          from vehicle_data
          group by enterprise,province
        ) as boxplot
),
totalVol_count as
(
  select
    tmp.enterprise,
    tmp.province,
    count(tmp.totalVol) as total
  from
      (
        select
          vehicle_data.enterprise,
          vehicle_data.province,
          vehicle_data.totalVol,
          totalVol_info.max_value,
          totalVol_info.min_value
        from vehicle_data
        join totalVol_info on vehicle_data.enterprise = totalVol_info.enterprise
        and  vehicle_data.province = totalVol_info.province
      ) as tmp
  where tmp.totalVol > tmp.max_value or tmp.totalVol < tmp.min_value
  group by tmp.enterprise,tmp.province
),
-- 温度
temp_info as
(
    select
        boxplot.enterprise,
        boxplot.province,
        (boxplot.q3 + boxplot.q1*${coefficient}) as max_value,
        (boxplot.q3 - boxplot.q1*${coefficient}) as min_value
    from
        (
          select
              enterprise,
              province,
              percentile_approx(temp, 0.75) as q3,
              percentile_approx(temp, 0.25) as q1
          from vehicle_data
          group by enterprise,province
        ) as boxplot
),
temp_count as
(
  select
    tmp.enterprise,
    tmp.province,
    count(tmp.temp) as total
  from
      (
        select
          vehicle_data.enterprise,
          vehicle_data.province,
          vehicle_data.temp,
          temp_info.max_value,
          temp_info.min_value
        from vehicle_data
        join temp_info on vehicle_data.enterprise = temp_info.enterprise
        and  vehicle_data.province = temp_info.province
      ) as tmp
  where tmp.temp > tmp.max_value or tmp.temp < tmp.min_value
  group by tmp.enterprise,tmp.province
),

-- 计算温差
tempDiff_info as
(
    select
        boxplot.enterprise,
        boxplot.province,
        (boxplot.q3 + boxplot.q1*${coefficient}) as max_value,
        (boxplot.q3 - boxplot.q1*${coefficient}) as min_value
    from
        (
          select
              enterprise,
              province,
              percentile_approx(tempDiff, 0.75) as q3,
              percentile_approx(tempDiff, 0.25) as q1
          from vehicle_data
          group by enterprise,province
        ) as boxplot
),
tempDiff_count as
(
  select
    tmp.enterprise,
    tmp.province,
    count(tmp.tempDiff) as total
  from
      (
        select
          vehicle_data.enterprise,
          vehicle_data.province,
          vehicle_data.tempDiff,
          tempDiff_info.max_value,
          tempDiff_info.min_value
        from vehicle_data
        join tempDiff_info on vehicle_data.enterprise = tempDiff_info.enterprise
        and  vehicle_data.province = tempDiff_info.province
      ) as tmp
  where tmp.tempDiff > tmp.max_value or tmp.tempDiff < tmp.min_value
  group by tmp.enterprise,tmp.province
),
-- 绝缘电阻统计
resistance_info as
(
    select
        boxplot.enterprise,
        boxplot.province,
        (boxplot.q3 + boxplot.q1*${coefficient}) as max_value,
        (boxplot.q3 - boxplot.q1*${coefficient}) as min_value
    from
        (
          select
              enterprise,
              province,
              percentile_approx(resistance, 0.75) as q3,
              percentile_approx(resistance, 0.25) as q1
          from vehicle_data
          group by enterprise,province
        ) as boxplot
),
resistance_count as
(
  select
    tmp.enterprise,
    tmp.province,
    count(tmp.resistance) as total
  from
      (
        select
          vehicle_data.enterprise,
          vehicle_data.province,
          vehicle_data.resistance,
          resistance_info.max_value,
          resistance_info.min_value
        from vehicle_data
        join resistance_info on vehicle_data.enterprise = resistance_info.enterprise
        and  vehicle_data.province = resistance_info.province
      ) as tmp
  where tmp.resistance > tmp.max_value or tmp.resistance < tmp.min_value
  group by tmp.enterprise,tmp.province
),

-- 总电流

totalCurrent_info as
(
    select
        boxplot.enterprise,
        boxplot.province,
        (boxplot.q3 + boxplot.q1*${coefficient}) as max_value,
        (boxplot.q3 - boxplot.q1*${coefficient}) as min_value
    from
        (
          select
              enterprise,
              province,
              percentile_approx(totalCurrent, 0.75) as q3,
              percentile_approx(totalCurrent, 0.25) as q1
          from vehicle_data
          group by enterprise,province
        ) as boxplot
),
totalCurrent_count as
(
  select
    tmp.enterprise,
    tmp.province,
    count(tmp.totalCurrent) as total
  from
      (
        select
          vehicle_data.enterprise,
          vehicle_data.province,
          vehicle_data.totalCurrent,
          totalCurrent_info.max_value,
          totalCurrent_info.min_value
        from vehicle_data
        join totalCurrent_info on vehicle_data.enterprise = totalCurrent_info.enterprise
        and  vehicle_data.province = totalCurrent_info.province
      ) as tmp
  where tmp.totalCurrent > tmp.max_value or tmp.totalCurrent < tmp.min_value
  group by tmp.enterprise,tmp.province
),
--

maxTemperatureRate_info as
(
    select
        boxplot.enterprise,
        boxplot.province,
        (boxplot.q3 + boxplot.q1*${coefficient}) as max_value,
        (boxplot.q3 - boxplot.q1*${coefficient}) as min_value
    from
        (
          select
              enterprise,
              province,
              percentile_approx(maxTemperatureRate, 0.75) as q3,
              percentile_approx(maxTemperatureRate, 0.25) as q1
          from vehicle_data
          group by enterprise,province
        ) as boxplot
),
maxTemperatureRate_count as
(
  select
    tmp.enterprise,
    tmp.province,
    count(tmp.maxTemperatureRate) as total
  from
      (
        select
          vehicle_data.enterprise,
          vehicle_data.province,
          vehicle_data.maxTemperatureRate,
          maxTemperatureRate_info.max_value,
          maxTemperatureRate_info.min_value
        from vehicle_data
        join maxTemperatureRate_info on vehicle_data.enterprise = maxTemperatureRate_info.enterprise
        and  vehicle_data.province = maxTemperatureRate_info.province
      ) as tmp
  where tmp.maxTemperatureRate > tmp.max_value or tmp.maxTemperatureRate < tmp.min_value
  group by tmp.enterprise,tmp.province
)

insert into table ${db}.outlier_statistic_perweek_es
select
  if(vc.enterprise is null,if(tvc.enterprise is null,if(tdc.enterprise is null,
   if(tpc.enterprise is null,if(rc.enterprise is null, if(mc.enterprise is null,
    if(ttc.enterprise is null,null,ttc.enterprise),mc.enterprise),rc.enterprise),
     tpc.enterprise),tdc.enterprise),tvc.enterprise),vc.enterprise),
  if(vc.province is null,if(tvc.province is null,if(tdc.province is null,
   if(tpc.province is null,if(rc.province is null, if(mc.province is null,
    if(ttc.province is null,null,ttc.province),mc.province),rc.province),
     tpc.province),tdc.province),tvc.province),vc.province),
  if(vc.total is null,0,vc.total),
  if(tvc.total is null,0,tvc.total),
  if(tdc.total is null,0,tdc.total),
  if(tpc.total is null,0,tpc.total),
  if(rc.total is null,0,rc.total),
  if(mc.total is null,0,mc.total),
  if(ttc.total is null,0,ttc.total),
  date_format('${endDate}','yyyy-MM-dd')
from  volDiff_count as vc
full outer join totalVol_count as tvc on vc.enterprise = tvc.enterprise and vc.province = tvc.province
full outer join tempDiff_count as tdc on vc.enterprise = tdc.enterprise and vc.province = tdc.province
full outer join temp_count as tpc on vc.enterprise = tpc.enterprise and vc.province = tpc.province
full outer join resistance_count as rc on vc.enterprise = rc.enterprise and vc.province = rc.province
full outer join maxTemperatureRate_count as mc on vc.enterprise = mc.enterprise and vc.province = mc.province
full outer join totalCurrent_count as ttc on vc.enterprise = ttc.enterprise and vc.province = ttc.province


"
hive -e  "${sql}"
