#!/bin/bash

db=warningplatform

# 获取当前时间
do_date=`date "+%Y-%m-%d %H:%M:%S"`

# 获取前一天的数据进行聚合
# dt为数据日期
sql="
with
warning_day as
  (
    select
      vin,
      province,
      warning_type,
      sum(total) total
    from ${db}.warning_info_statistic_perhour
    where year = date_format(date_add('${do_date}',-1),'yyyy')
    and  month = date_format(date_add('${do_date}',-1),'MM')
    and   day  = date_format(date_add('${do_date}',-1),'dd')
    group by vin,province,warning_type
  ),
type_enterprise as
  (
    select
        vin,
        vehicle_type,
        enterprise
    from  ${db}.warning_info_statistic_perhour
    where year = date_format(date_add('${do_date}',-1),'yyyy')
    and  month = date_format(date_add('${do_date}',-1),'MM')
    and   day  = date_format(date_add('${do_date}',-1),'dd')
    group by vin,vehicle_type,enterprise
  )

insert into table ${db}.warning_info_statistic_es_perday
select
  warning_day.vin,
  type_enterprise.vehicle_type,
  type_enterprise.enterprise,
  warning_day.province,
  warning_day.warning_type,
  warning_day.total,
  date_format(date_add('${do_date}',-1),'yyyy-MM-dd') dt
from warning_day join type_enterprise
on warning_day.vin = type_enterprise.vin;
"

hive -e "${sql}"
