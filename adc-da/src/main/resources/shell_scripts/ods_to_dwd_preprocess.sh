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
    get_json_object(data,'$.speed'),
    get_json_object(data,'$.startupStatus'),
    get_json_object(data,'$.runMode'),
    get_json_object(data,'$.odo'),
    get_json_object(data,'$.gearStatus'),
    get_json_object(data,'$.chargeStatus'),
    get_json_object(data,'$.maxCellVoltageNum'),
    get_json_object(data,'$.maxCellVoltage'),
    get_json_object(data,'$.minCellVoltageNum'),
    get_json_object(data,'$.minCellVoltage'),
    get_json_object(data,'$.maxProbeTemperatureNum'),
    get_json_object(data,'$.maxProbeTemperature'),
    get_json_object(data,'$.minProbeTemperatureNum'),
    get_json_object(data,'$.minProbeTemperature'),
    get_json_object(data,'$.cellVoltage'),
    get_json_object(data,'$.differenceCellVoltage'),
    get_json_object(data,'$.maxTemperatureRate'),
    get_json_object(data,'$.temperatureRate'),
    get_json_object(data,'$.atanMaxTemperatureRate'),
    get_json_object(data,'$.atanMinTemperatureRate'),
    get_json_object(data,'$.averageProbeTemperature'),
    get_json_object(data,'$.averageCellVoltage'),
    get_json_object(data,'$.varianceCellVoltage'),
    get_json_object(data,'$.varianceProbeTemperature'),
    get_json_object(data,'$.entropy'),
    get_json_object(data,'$.variation'),
    get_json_object(data,'$.wDifferenceCellVoltages'),
    get_json_object(data,'$.wDifferenceTotalCellVoltage'),
    get_json_object(data,'$.differenceInternalResistance'),
    get_json_object(data,'$.averageModuleCellVoltages'),
    get_json_object(data,'$.maxModuleCellVoltages'),
    get_json_object(data,'$.minModuleCellVoltages'),
    get_json_object(data,'$.maxModuleCellVoltageNums'),
    get_json_object(data,'$.minModuleCellVoltageNums'),
    get_json_object(data,'$.totalModuleCellVoltages'),
    get_json_object(data,'$.differenceModuleCellVoltages'),
    get_json_object(data,'$.instantaneousConsumption'),
    get_json_object(data,'$.wDischargeRate'),
    get_json_object(data,'$.resistance'),
    get_json_object(data,'$.province'),
    get_json_object(data,'$.city'),
    get_json_object(data,'$.country'),
    get_json_object(data,'$.vehicleType'),
    get_json_object(data,'$.enterprise'),
    get_json_object(data,'$.totalCurrent'),
    cast (substring(get_json_object(data,'$.soc'),0,length(get_json_object(data,'$.soc'))-1) as double)/100
from ${db}.ods_preprocess_vehicle_data
where dt = '${do_date}';
"
hive -e  "${sql}"





