#!/bin/bash
db=warningplatform
# 计算方式有待确认，后续完成
sql="
with
charge_data as
(
  select

  from ${db}.ods_preprocess_vehicle_data
  where dt =

)
"
hive  -e "${sql}"