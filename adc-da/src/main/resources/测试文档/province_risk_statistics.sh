#!/bin/bash

db=warningplatform
# 获取当前日期
do_date=`date  "+%Y-%m-%d %H:%M:%S"`

if [[ -n "$1" ]]; then
    do_date="${1}"
fi


sql="
with
-- 按照企业，省份，风险等级来统计未审核的风险个数
warning as
(select
      enterprise,
      province,
      risk_level,
      count(*) total
    from ${db}.battery_warning_info_es
    where review_status = '1' or review_status = '2' or review_status = '3'
    group by enterprise,province,risk_level
),


warning_level as
(select
    warning.enterprise,
    warning.province,
    case warning.risk_level when '1' then warning.total else 0 end r1,
    case warning.risk_level when '2' then warning.total else 0 end r2,
    case warning.risk_level when '3' then warning.total else 0 end r3
from warning
)

insert into table ${db}.province_warning_statistic_es
select
  warning_level.enterprise,
  warning_level.province,
  sum(warning_level.r1) r1,
  sum(warning_level.r2) r2,
  sum(warning_level.r3) r3,
  0,
  date_format('${do_date}','yyyy-MM-dd HH:mm:ss')
from warning_level
group by warning_level.enterprise,warning_level.province;
"
hive -e  "${sql}"

