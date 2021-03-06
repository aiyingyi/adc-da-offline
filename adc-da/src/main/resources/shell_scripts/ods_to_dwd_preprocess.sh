#!/bin/bash

# 将预处理的原始数据ods_preprocess_vehicle_data解析到dwd_preprocess_vehicle_data
# 每天执行一次
db=warningplatform

# 获取当前日期
do_date=`date -d "1 day ago" "+%Y-%m-%d"`

# 计算之前应该导入前一天的数据到dwd_preprocess_vehicle_data中
sql="
insert into table ${db}.dwd_preprocess_vehicle_data partition(dt='${do_date}')
select
    get_json_object(data,'$.vin'),
    get_json_object(data,'$.msgTime'),
    cast (get_json_object(data,'$.speed') as double),
    get_json_object(data,'$.startupStatus'),
    get_json_object(data,'$.runMode'),
    cast (get_json_object(data,'$.odo') as double),
    get_json_object(data,'$.gearStatus'),
    get_json_object(data,'$.chargeStatus'),
    get_json_object(data,'$.maxCellVoltageNum'),
    cast (get_json_object(data,'$.maxCellVoltage') as double),
    get_json_object(data,'$.minCellVoltageNum'),
    cast (get_json_object(data,'$.minCellVoltage') as double),
    get_json_object(data,'$.maxProbeTemperatureNum'),
    cast (get_json_object(data,'$.maxProbeTemperature') as double),
    get_json_object(data,'$.minProbeTemperatureNum'),
    cast (get_json_object(data,'$.minProbeTemperature') as double),
    get_json_object(data,'$.cellVoltage'),
    cast (get_json_object(data,'$.differenceCellVoltage') as double),
    cast (get_json_object(data,'$.maxTemperatureRate') as double),
    get_json_object(data,'$.temperatureRate'),
    cast (get_json_object(data,'$.atanMaxTemperatureRate') as double),
    cast (get_json_object(data,'$.atanMinTemperatureRate') as double),
    cast (get_json_object(data,'$.averageProbeTemperature') as double),
    cast (get_json_object(data,'$.averageCellVoltage') as double),
    cast (get_json_object(data,'$.varianceCellVoltage') as double),
    cast (get_json_object(data,'$.varianceProbeTemperature') as double),
    cast (get_json_object(data,'$.entropy') as double),
    cast (get_json_object(data,'$.variation') as double),
    get_json_object(data,'$.wDifferenceCellVoltages'),
    cast (get_json_object(data,'$.wDifferenceTotalCellVoltage')  as double),
    cast (get_json_object(data,'$.differenceInternalResistance') as double),
    get_json_object(data,'$.averageModuleCellVoltages'),
    get_json_object(data,'$.maxModuleCellVoltages'),
    get_json_object(data,'$.minModuleCellVoltages'),
    get_json_object(data,'$.maxModuleCellVoltageNums'),
    get_json_object(data,'$.minModuleCellVoltageNums'),
    get_json_object(data,'$.totalModuleCellVoltages'),
    get_json_object(data,'$.differenceModuleCellVoltages'),
    cast (get_json_object(data,'$.instantaneousConsumption') as double),
    cast (get_json_object(data,'$.wDischargeRate') as double),
    cast (get_json_object(data,'$.resistance') as double),
    get_json_object(data,'$.province'),
    get_json_object(data,'$.city'),
    get_json_object(data,'$.country'),
    get_json_object(data,'$.vehicleType'),
    get_json_object(data,'$.enterprise'),
    cast (get_json_object(data,'$.totalCurrent') as double),
    cast (substring(get_json_object(data,'$.soc'),0,length(get_json_object(data,'$.soc'))-1) as double)/100
from ${db}.ods_preprocess_vehicle_data
where dt = '${do_date}';
"
hive -e  "${sql}"





