#!/bin/bash

db=warningplatform

sql="
with
charge_data as
(
  seelect

  from ${db}.ods_preprocess_vehicle_data
  where dt = spark

)
"
hive  -e "${sql}"