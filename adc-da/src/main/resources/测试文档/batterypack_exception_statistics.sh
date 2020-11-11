#!/bin/bash

# 每周执行一次
# named_struct 会把整数直接当成int类型，从而不能插入double类型中

db=warningplatform

coefficient=1.5

# 获取当前日期
do_date=`date  "+%Y-%m-%d %H:%M:%S"`

if [[ -n "$1" ]]; then
    do_date="$1"
fi

# 计算之前应该导入前一天的数据到dwd_preprocess_vehicle_data中
# 首先执行 ods_to_dwd_preprocess.sh 脚本，将前一天的数据导入到dwd层
sql="
-- 按照企业，省份，车型来计算每辆车的平均值
insert overwrite  table  ${db}.avg_vehicle_data_perweek
 select
      enterprise,
      province,
      vehicleType,
      vin,
      round(avg(differenceCellVoltage),1)  diff_Voltage,
      round(avg(maxProbeTemperature-minProbeTemperature),1) diff_temper,
      round(avg(maxTemperatureRate),1) temper_rate,
      round(avg(averageProbeTemperature),1) temper,
      round(avg(resistance),1) resistance,
      round(avg(wDischargeRate),1) wDischargeRate
  from ${db}.dwd_preprocess_vehicle_data
  where dt >= date_format(date_add(next_day('${do_date}','MO'),-14),'yyyy-MM-dd')
  and dt <= date_format(date_add(next_day('${do_date}','SU'),-7),'yyyy-MM-dd')
  group by enterprise,province,vehicleType,vin;

with
avg_data_rank as
(
  select
    enterprise,
    province,
    vehicleType,
    diff_Voltage,
    lead(diff_Voltage,1, 0) over(partition by enterprise,province,vehicleType order by diff_Voltage)   next_diff_vol,
    row_number() over(partition by enterprise,province,vehicleType order by diff_Voltage)   Vol_rk,
    temper_rate,
    lead(temper_rate,1, 0) over(partition by enterprise,province,vehicleType order by temper_rate)   next_temper_rate,
    row_number() over(partition by enterprise,province,vehicleType order by temper_rate)   temper_rate_rk,
    temper,
    lead(temper,1, 0) over(partition by enterprise,province,vehicleType order by temper)   next_temper,
    row_number() over(partition by enterprise,province,vehicleType order by temper)   temper_rk,
    diff_temper,
    lead(diff_temper,1, 0) over(partition by enterprise,province,vehicleType order by diff_temper)   next_diff_temper,
    row_number() over(partition by enterprise,province,vehicleType order by diff_temper)   diff_temper_rk,
    resistance,
    lead(resistance,1, 0) over(partition by enterprise,province,vehicleType order by resistance)   next_resistance,
    row_number() over(partition by enterprise,province,vehicleType order by resistance)   resistance_rk,
    wDischargeRate,
    lead(wDischargeRate,1, 0) over(partition by enterprise,province,vehicleType order by wDischargeRate)   next_wDischargeRate,
    row_number() over(partition by enterprise,province,vehicleType order by wDischargeRate)   wDischargeRate_rk
  from  ${db}.avg_vehicle_data_perweek
),
particle_location as    -- 根据企业，省份，车型去计算四分位数据的位置
(
    select
        enterprise,
        province,
        vehicleType,
        cast(3*(count(*) + 1) / 4 as int) as q3_int_part,
        (count(*)+1)*3 / 4 % 1            as q3_float_part,
        cast((count(*) + 1) / 2 as int)   as q2_int_part,
        (count(*)+1) / 2 % 1              as q2_float_part,
        cast((count(*) + 1) / 4 as int)   as q1_int_part,
        (count(*)+1) / 4 % 1              as q1_float_part
    from  avg_data_rank
    group by  enterprise,province,vehicleType
),
-- 按照企业，省份，车型，计算压差的箱线值
diff_Voltage_info as
(
    select
        q3.enterprise,
        q3.province,
        q3.vehicleType,
        q3.vol_q3,
        q2.vol_q2,
        q1.vol_q1,
        (q3.vol_q3 + q1.vol_q1*${coefficient}) as vol_max_value,
        (q3.vol_q3 - q1.vol_q1*${coefficient}) as vol_min_value
    from
          (select
            adr.enterprise,
            adr.province,
            adr.vehicleType,
            adr.diff_Voltage *(1-pl.q3_float_part) + adr.next_diff_vol * pl.q3_float_part as vol_q3
          from avg_data_rank adr join particle_location pl
          on adr.enterprise = pl.enterprise and adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.Vol_rk = pl.q3_int_part) q3
    join
          (select
            adr.enterprise,
            adr.province,
            adr.vehicleType,
            adr.diff_Voltage *(1-pl.q2_float_part) + adr.next_diff_vol * pl.q2_float_part as vol_q2
          from avg_data_rank adr join particle_location pl
          on  adr.enterprise = pl.enterprise and adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.Vol_rk = pl.q2_int_part) q2
    on q3.enterprise = q2.enterprise and q3.province = q2.province and q3.vehicleType = q2.vehicleType
    join
           (select
            adr.enterprise,
            adr.province,
            adr.vehicleType,
            adr.diff_Voltage *(1-pl.q1_float_part) + adr.next_diff_vol * pl.q1_float_part as vol_q1
          from avg_data_rank adr join particle_location pl
          on adr.enterprise = pl.enterprise and adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.Vol_rk = pl.q1_int_part) q1
    on q3.enterprise = q1.enterprise and q3.province = q1.province and q3.vehicleType = q1.vehicleType
),
temper_rate_info as
(
    select
        q3.enterprise,
        q3.province,
        q3.vehicleType,
        q3.temper_rate_q3,
        q2.temper_rate_q2,
        q1.temper_rate_q1,
        (q3.temper_rate_q3 + q1.temper_rate_q1*${coefficient}) as temper_rate_max_value,
        (q3.temper_rate_q3 - q1.temper_rate_q1*${coefficient}) as temper_rate_min_value
    from
          (select
            adr.enterprise,
            adr.province,
            adr.vehicleType,
            adr.temper_rate *(1-pl.q3_float_part) + adr.next_temper_rate * pl.q3_float_part as temper_rate_q3
          from avg_data_rank adr join particle_location pl
          on adr.enterprise = pl.enterprise and adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.temper_rate_rk = pl.q3_int_part) q3
    join
          (select
            adr.enterprise,
            adr.province,
            adr.vehicleType,
            adr.temper_rate *(1-pl.q2_float_part) + adr.next_temper_rate * pl.q2_float_part as temper_rate_q2
          from avg_data_rank adr join particle_location pl
          on adr.enterprise = pl.enterprise and adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.temper_rate_rk = pl.q2_int_part) q2
    on q3.enterprise = q2.enterprise and q3.province = q2.province and q3.vehicleType = q2.vehicleType
    join
          (select
            adr.enterprise,
            adr.province,
            adr.vehicleType,
            adr.temper_rate *(1-pl.q1_float_part) + adr.next_temper_rate * pl.q1_float_part as temper_rate_q1
          from avg_data_rank adr join particle_location pl
          on adr.enterprise = pl.enterprise and adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.temper_rate_rk = pl.q1_int_part) q1
    on q3.enterprise = q1.enterprise and q3.province = q1.province and q3.vehicleType = q1.vehicleType
),
temper_info as
(
    select
        q3.enterprise,
        q3.province,
        q3.vehicleType,
        q3.temper_q3,
        q2.temper_q2,
        q1.temper_q1,
        (q3.temper_q3 + q1.temper_q1*${coefficient}) as temper_max_value,
        (q3.temper_q3 - q1.temper_q1*${coefficient}) as temper_min_value
    from
          (select
            adr.enterprise,
            adr.province,
            adr.vehicleType,
            adr.temper *(1-pl.q3_float_part) + adr.next_temper * pl.q3_float_part as temper_q3
          from avg_data_rank adr join particle_location pl
          on adr.enterprise = pl.enterprise and adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.temper_rk = pl.q3_int_part) q3
    join
          (select
            adr.enterprise,
            adr.province,
            adr.vehicleType,
            adr.temper *(1-pl.q2_float_part) + adr.next_temper * pl.q2_float_part as temper_q2
          from avg_data_rank adr join particle_location pl
          on adr.enterprise = pl.enterprise and adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.temper_rk = pl.q2_int_part) q2
    on q3.enterprise = q2.enterprise and q3.province = q2.province and q3.vehicleType = q2.vehicleType
    join
          (select
            adr.enterprise,
            adr.province,
            adr.vehicleType,
            adr.temper *(1-pl.q1_float_part) + adr.next_temper * pl.q1_float_part as temper_q1
          from avg_data_rank adr join particle_location pl
          on adr.enterprise = pl.enterprise and  adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.temper_rk = pl.q1_int_part) q1
    on q3.enterprise = q1.enterprise and q3.province = q1.province and q3.vehicleType = q1.vehicleType
),
diff_temper_info as
(
    select
        q3.enterprise,
        q3.province,
        q3.vehicleType,
        q3.diff_temper_q3,
        q2.diff_temper_q2,
        q1.diff_temper_q1,
        (q3.diff_temper_q3 + q1.diff_temper_q1*${coefficient}) as diff_temper_max_value,
        (q3.diff_temper_q3 - q1.diff_temper_q1*${coefficient}) as diff_temper_min_value
    from
          (select
            adr.enterprise,
            adr.province,
            adr.vehicleType,
            adr.diff_temper *(1-pl.q3_float_part) + adr.next_diff_temper * pl.q3_float_part as diff_temper_q3
          from avg_data_rank adr join particle_location pl
          on adr.enterprise = pl.enterprise and adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.diff_temper_rk = pl.q3_int_part) q3
    join
          (select
            adr.enterprise,
            adr.province,
            adr.vehicleType,
            adr.diff_temper *(1-pl.q2_float_part) + adr.next_diff_temper * pl.q2_float_part as diff_temper_q2
          from avg_data_rank adr join particle_location pl
          on adr.enterprise = pl.enterprise and adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.diff_temper_rk = pl.q2_int_part) q2
    on q3.enterprise = q2.enterprise and q3.province = q2.province and q3.vehicleType = q2.vehicleType
    join
           (select
            adr.enterprise,
            adr.province,
            adr.vehicleType,
            adr.diff_temper *(1-pl.q1_float_part) + adr.next_diff_temper * pl.q1_float_part as diff_temper_q1
          from avg_data_rank adr join particle_location pl
          on adr.enterprise = pl.enterprise and adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.diff_temper_rk = pl.q1_int_part) q1
    on q3.enterprise = q1.enterprise and q3.province = q1.province and q3.vehicleType = q1.vehicleType
),
resistance_info as
(
    select
        q3.enterprise,
        q3.province,
        q3.vehicleType,
        q3.resistance_q3,
        q2.resistance_q2,
        q1.resistance_q1,
        (q3.resistance_q3 + q1.resistance_q1*${coefficient}) as resistance_max_value,
        (q3.resistance_q3 - q1.resistance_q1*${coefficient}) as resistance_min_value
    from
          (select
            adr.enterprise,
            adr.province,
            adr.vehicleType,
            adr.resistance *(1-pl.q3_float_part) + adr.next_resistance * pl.q3_float_part as resistance_q3
          from avg_data_rank adr join particle_location pl
          on adr.enterprise = pl.enterprise and adr.enterprise = pl.enterprise and adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.resistance_rk = pl.q3_int_part) q3
    join
          (select
            adr.enterprise,
            adr.province,
            adr.vehicleType,
            adr.resistance *(1-pl.q2_float_part) + adr.next_resistance * pl.q2_float_part as resistance_q2
          from avg_data_rank adr join particle_location pl
          on adr.enterprise = pl.enterprise and adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.resistance_rk = pl.q2_int_part) q2
    on q3.enterprise = q2.enterprise and q3.province = q2.province and q3.vehicleType = q2.vehicleType
    join
          (select
            adr.enterprise,
            adr.province,
            adr.vehicleType,
            adr.resistance *(1-pl.q1_float_part) + adr.next_resistance * pl.q1_float_part as resistance_q1
          from avg_data_rank adr join particle_location pl
          on adr.enterprise = pl.enterprise and  adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.resistance_rk = pl.q1_int_part) q1
    on q3.enterprise = q1.enterprise and q3.province = q1.province and q3.vehicleType = q1.vehicleType
),
wDischargeRate_info as
(
    select
        q3.enterprise,
        q3.province,
        q3.vehicleType,
        q3.wDischargeRate_q3,
        q2.wDischargeRate_q2,
        q1.wDischargeRate_q1,
        (q3.wDischargeRate_q3 + q1.wDischargeRate_q1*${coefficient}) as wDischargeRate_max_value,
        (q3.wDischargeRate_q3 - q1.wDischargeRate_q1*${coefficient}) as wDischargeRate_min_value
    from
          (select
           adr.enterprise,
            adr.province,
            adr.vehicleType,
            adr.wDischargeRate *(1-pl.q3_float_part) + adr.next_wDischargeRate * pl.q3_float_part as wDischargeRate_q3
          from avg_data_rank adr join particle_location pl
          on adr.enterprise = pl.enterprise and adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.wDischargeRate_rk = pl.q3_int_part) q3
    join
          (select
            adr.enterprise,
            adr.province,
            adr.vehicleType,
            adr.wDischargeRate *(1-pl.q2_float_part) + adr.next_wDischargeRate * pl.q2_float_part as wDischargeRate_q2
          from avg_data_rank adr join particle_location pl
          on adr.enterprise = pl.enterprise and adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.wDischargeRate_rk = pl.q2_int_part) q2
    on q3.enterprise = q2.enterprise and q3.province = q2.province and q3.vehicleType = q2.vehicleType
    join
          (select
            adr.enterprise,
            adr.province,
            adr.vehicleType,
            adr.wDischargeRate *(1-pl.q1_float_part) + adr.next_wDischargeRate * pl.q1_float_part as wDischargeRate_q1
          from avg_data_rank adr join particle_location pl
          on adr.enterprise = pl.enterprise and adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.wDischargeRate_rk = pl.q1_int_part) q1
    on q3.enterprise = q1.enterprise and q3.province = q1.province and q3.vehicleType = q1.vehicleType

),
diff_Voltage_statistics as
(
    select
        tmp.enterprise,
        tmp.province,
        tmp.vehicleType,
        round(diff_Voltage_info.vol_q3,1) vol_q3,
        round(diff_Voltage_info.vol_q2,1) vol_q2,
        round(diff_Voltage_info.vol_q1,1) vol_q1,
        round(diff_Voltage_info.vol_max_value,1) vol_max_value,
        round(diff_Voltage_info.vol_min_value,1) vol_min_value,
        tmp.vehicles
    from
        (
        select
            avdp.enterprise,
            avdp.province,
            avdp.vehicleType,
            collect_list(named_struct('vin',vin,'outliers',avdp.diff_Voltage))  as vehicles
        from ${db}.avg_vehicle_data_perweek avdp join diff_Voltage_info dvi
        on avdp.enterprise = dvi.enterprise and avdp.province = dvi.province and avdp.vehicleType = dvi.vehicleType
        where avdp.diff_Voltage > round(dvi.vol_max_value,1)
        group by avdp.enterprise,avdp.province,avdp.vehicleType)  tmp
    join diff_Voltage_info
    on  tmp.enterprise = diff_Voltage_info.enterprise and tmp.province = diff_Voltage_info.province and tmp.vehicleType = diff_Voltage_info.vehicleType
),
-- 温差统计数据
diff_temper_statistics as
(
    select
        tmp.enterprise,
        tmp.province,
        tmp.vehicleType,
        round(diff_temper_info.diff_temper_q3,1) diff_temper_q3,
        round(diff_temper_info.diff_temper_q2,1) diff_temper_q2,
        round(diff_temper_info.diff_temper_q1,1) diff_temper_q1,
        round(diff_temper_info.diff_temper_max_value,1) diff_temper_max_value,
        round(diff_temper_info.diff_temper_min_value,1) diff_temper_min_value,
        tmp.vehicles
    from
        (
        select
            avdp.enterprise,
            avdp.province,
            avdp.vehicleType,
            collect_list(named_struct('vin',vin,'outliers',avdp.diff_temper))  as vehicles
        from ${db}.avg_vehicle_data_perweek avdp join diff_temper_info dti
        on avdp.enterprise = dti.enterprise and avdp.province = dti.province and avdp.vehicleType = dti.vehicleType
        where avdp.diff_temper > round(dti.diff_temper_max_value,1)
        group by avdp.enterprise,avdp.province,avdp.vehicleType)  tmp
    join diff_temper_info  on tmp.enterprise = diff_temper_info.enterprise and  tmp.province = diff_temper_info.province and tmp.vehicleType = diff_temper_info.vehicleType
),
temper_rate_statistics as
(
    select
        tmp.enterprise,
        tmp.province,
        tmp.vehicleType,
        round(temper_rate_info.temper_rate_q3,1) temper_rate_q3,
        round(temper_rate_info.temper_rate_q2,1) temper_rate_q2,
        round(temper_rate_info.temper_rate_q1,1) temper_rate_q1,
        round(temper_rate_info.temper_rate_max_value,1) temper_rate_max_value,
        round(temper_rate_info.temper_rate_min_value,1) temper_rate_min_value,
        tmp.vehicles
    from
        (
        select
            avdp.enterprise,
            avdp.province,
            avdp.vehicleType,
            collect_list(named_struct('vin',vin,'outliers',avdp.temper_rate))  as vehicles
        from ${db}.avg_vehicle_data_perweek avdp join temper_rate_info tri
        on avdp.enterprise = tri.enterprise and avdp.province = tri.province and avdp.vehicleType = tri.vehicleType
        where avdp.temper_rate > round(tri.temper_rate_max_value,1)
        group by avdp.enterprise,avdp.province,avdp.vehicleType)  tmp
    join temper_rate_info  on  tmp.enterprise = temper_rate_info.enterprise  and  tmp.province = temper_rate_info.province and tmp.vehicleType = temper_rate_info.vehicleType
),
temper_statistics as
(
    select
        tmp.enterprise,
        tmp.province,
        tmp.vehicleType,
        round(temper_info.temper_q3,1) temper_q3,
        round(temper_info.temper_q2,1) temper_q2,
        round(temper_info.temper_q1,1) temper_q1,
        round(temper_info.temper_max_value,1) temper_max_value,
        round(temper_info.temper_min_value,1) temper_min_value,
        tmp.vehicles
    from
        (
        select
            avdp.enterprise,
            avdp.province,
            avdp.vehicleType,
            collect_list(named_struct('vin',vin,'outliers',avdp.temper))  as vehicles
        from ${db}.avg_vehicle_data_perweek avdp join temper_info ti
        on avdp.enterprise = ti.enterprise and avdp.province = ti.province and avdp.vehicleType = ti.vehicleType
        where avdp.temper > round(ti.temper_max_value,1)
        group by avdp.enterprise,avdp.province,avdp.vehicleType)  tmp
    join temper_info  on  tmp.enterprise = temper_info.enterprise and tmp.province = temper_info.province and tmp.vehicleType = temper_info.vehicleType
),
resistance_statistics as
(
    select
        tmp.enterprise,
        tmp.province,
        tmp.vehicleType,
        round(resistance_info.resistance_q3,1) resistance_q3,
        round(resistance_info.resistance_q2,1) resistance_q2,
        round(resistance_info.resistance_q1,1) resistance_q1,
        round(resistance_info.resistance_max_value,1) resistance_max_value,
        round(resistance_info.resistance_min_value,1) resistance_min_value,
        tmp.vehicles
    from
        (
        select
            avdp.enterprise,
            avdp.province,
            avdp.vehicleType,
            collect_list(named_struct('vin',vin,'outliers',avdp.resistance))  as vehicles
        from ${db}.avg_vehicle_data_perweek avdp join resistance_info ri
        on avdp.enterprise = ri.enterprise and avdp.province = ri.province and avdp.vehicleType = ri.vehicleType
        where avdp.resistance < round(ri.resistance_min_value,1)
        group by avdp.enterprise,avdp.province,avdp.vehicleType)  tmp
    join resistance_info  on  tmp.enterprise = resistance_info.enterprise and tmp.province = resistance_info.province and tmp.vehicleType = resistance_info.vehicleType
),
wDischargeRate_statistics as
(
    select
        tmp.enterprise,
        tmp.province,
        tmp.vehicleType,
        round(wDischargeRate_info.wDischargeRate_q3,1) wDischargeRate_q3,
        round(wDischargeRate_info.wDischargeRate_q2,1) wDischargeRate_q2,
        round(wDischargeRate_info.wDischargeRate_q1,1) wDischargeRate_q1,
        round(wDischargeRate_info.wDischargeRate_max_value,1) wDischargeRate_max_value,
        round(wDischargeRate_info.wDischargeRate_min_value,1) wDischargeRate_min_value,
        tmp.vehicles
    from
        (
        select
            avdp.enterprise,
            avdp.province,
            avdp.vehicleType,
            collect_list(named_struct('vin',vin,'outliers',avdp.wDischargeRate))  as vehicles
        from ${db}.avg_vehicle_data_perweek avdp join wDischargeRate_info wi
        on avdp.enterprise = wi.enterprise and avdp.province = wi.province and avdp.vehicleType = wi.vehicleType
        where avdp.wDischargeRate > round(wi.wDischargeRate_max_value,1)
        group by avdp.enterprise,avdp.province,avdp.vehicleType)  tmp
    join wDischargeRate_info  on  tmp.enterprise = wDischargeRate_info.enterprise and tmp.province = wDischargeRate_info.province and tmp.vehicleType = wDischargeRate_info.vehicleType
)

insert into table ${db}.batterypack_exception_es
select
    dvs.enterprise,
    dvs.province,
    dvs.vehicleType,
    named_struct('Q3',dvs.vol_q3,'Q2',dvs.vol_q2,'Q1',dvs.vol_q1,'maxvalue',dvs.vol_max_value,'minvalue',dvs.vol_min_value,'vehicles',dvs.vehicles) as vol_diff_exception,
    named_struct('Q3',trs.temper_rate_q3,'Q2',trs.temper_rate_q2,'Q1',trs.temper_rate_q1,'maxvalue',trs.temper_rate_max_value,'minvalue',trs.temper_rate_min_value,'vehicles',trs.vehicles) as temper_rate_exception,
    named_struct('Q3',ts.temper_q3,'Q2',ts.temper_q2,'Q1',ts.temper_q1,'maxvalue',ts.temper_max_value,'minvalue',ts.temper_min_value,'vehicles',ts.vehicles) as temper_exception,
    named_struct('Q3',dts.diff_temper_q3,'Q2',dts.diff_temper_q2,'Q1',dts.diff_temper_q1,'maxvalue',dts.diff_temper_max_value,'minvalue',dts.diff_temper_min_value,'vehicles',dts.vehicles) as temper_diff_exception,
    named_struct('Q3',rs.resistance_q3,'Q2',rs.resistance_q2,'Q1',rs.resistance_q1,'maxvalue',rs.resistance_max_value,'minvalue',rs.resistance_min_value,'vehicles',rs.vehicles) as resistance_exception,
    named_struct('Q3',wds.wDischargeRate_q3,'Q2',wds.wDischargeRate_q2,'Q1',wds.wDischargeRate_q1,'maxvalue',wds.wDischargeRate_max_value,'minvalue',wds.wDischargeRate_min_value,'vehicles',wds.vehicles) as discharge_rate_exception,
    date_format(date_add(next_day('${do_date}','MO'),-14),'yyyy-MM-dd') as dt
from  diff_Voltage_statistics   dvs
join  temper_rate_statistics    trs on  dvs.enterprise = trs.enterprise and dvs.province = trs.province and dvs.vehicleType = trs.vehicleType
join  temper_statistics         ts  on  dvs.enterprise = ts.enterprise and dvs.province = ts.province and dvs.vehicleType = ts.vehicleType
join  diff_temper_statistics    dts on  dvs.enterprise = dts.enterprise and  dvs.province = dts.province and dvs.vehicleType = dts.vehicleType
join  resistance_statistics     rs  on  dvs.enterprise = rs.enterprise  and dvs.province = rs.province and dvs.vehicleType = rs.vehicleType
join  wDischargeRate_statistics wds on  dvs.enterprise = wds.enterprise and dvs.province = wds.province and dvs.vehicleType = wds.vehicleType;
"
hive -e  "${sql}"





