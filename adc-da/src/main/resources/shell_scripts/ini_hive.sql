-- 创建Hive 离线数据库

CREATE DATABASE IF NOT EXISTS warningplatform LOCATION '/warningplatform.db';

USE warningplatform;


-- 创建预警类型维度表
create external table warning_type
(
    id        int,
    type_name string,
    level_id  int comment '预警等级id'
) row format delimited fields terminated by '\t'
    location '/warningplatform.db/dwd/warning_type';


-- 创建故障类型维度表
create external table failure_type
(
    id        int,
    type_name string
) row format delimited fields terminated by '\t'
    location '/warningplatform.db/dwd/failure_type';


-- 创建预警等级维度表
create external table warning_level
(
    id         int,
    level_type string
) row format delimited fields terminated by '\t'
    location '/warningplatform.db/dwd/warning_level';


-- 创建预警信息es映射表
create external table battery_warning_info_es
(
    vin                string,
    vehicle_type       string,
    enterprise         string,
    license_plate      string,
    battery_type       string,
    risk_level         string,
    province           string,
    warning_start_time string,
    warning_end_time   string,
    warning_type       string,
    lose_efficacy_type string,
    review_status      string,
    review_result      string,
    review_user        string
) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
    location '/warningplatform.db/ads/battery_warning_info_es'
    TBLPROPERTIES ('es.resource' = 'warning/warning',
        'es.mapping.names' =
                'vin:vin,vehicle_type:vehicleType,enterprise:enterprise,license_plate:licensePlate,battery_type:batteryType,risk_level:riskLevel,province:province,warning_start_time:warningStartTime,warning_end_time:warningEndTime,warning_type:warningType,lose_efficacy_type:loseEfficacyType,review_status:reviewStatus,review_result:reviewResult,review_user:reviewUser',
        'es.nodes' = '192.168.11.29',
        'es.port' = '9200'
        );


-- 创建预警信息表，每小时从es中拉取一次数据，不用分区，直接覆盖掉即可,拉取上一个小时的数据

create external table battery_warning_info_perhour
(
    vin          string,
    vehicle_type string,
    enterprise   String,
    province     string,
    warning_type string comment '预警类型',
    dt           string
) row format delimited fields terminated by '\t'
    location '/warningplatform.db/dwt/battery_warning_info_perhour';

-- 创建预警信息统计表，按照小时进行统计，按照时间分区
create external table warning_info_statistic_perhour
(
    vin          string,
    vehicle_type string,
    enterprise   String,
    province     string,
    warning_type string comment '预警类型',
    total        bigint comment '故障次数',
    dt           string comment '本次统计范围的开始整点'
) partitioned by (year string,month string,day string)
    row format delimited fields terminated by '\t'
    location '/warningplatform.db/ads/warning_info_statistic_perhour';


-- 创建预警统计信息表与es每小时统计表的映射表
-- 注意添加hive写入es的两个jar包

CREATE EXTERNAL TABLE warning_info_statistic_es_perhour
(
    vin          string,
    vehicle_type string,
    enterprise   String,
    province     string,
    warning_type string comment '预警类型',
    total        bigint comment '故障次数',
    dt           string comment '本次统计范围的开始整点'
)
    STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
    location '/warningplatform.db/ads/warning_info_statistic_es_perhour'
    TBLPROPERTIES ('es.resource' = 'warninginfo_statistic_perhour/warninginfo_statistic_perhour',
        'es.mapping.names' =
                'vin:vin,vehicle_type:vehicleType,enterprise:enterprise,warning_type:warningType,total:total,dt:dt',
        'es.nodes' = '192.168.11.29',
        'es.port' = '9200',
        'es.index.auto.create' = 'TRUE'
        );


-- 创建预警统计信息表与es每天统计表的映射表

CREATE EXTERNAL TABLE warning_info_statistic_es_perday
(
    vin          string,
    vehicle_type string,
    enterprise   String,
    province     string,
    warning_type string comment '预警类型',
    total        bigint comment '故障次数',
    dt           string comment '统计数据的日期'
)
    STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
    location '/warningplatform.db/ads/warning_info_statistic_es_perday'
    TBLPROPERTIES ('es.resource' = 'warninginfo_statistic_perday/warninginfo_statistic_perday',
        'es.mapping.names' =
                'vin:vin,vehicle_type:vehicleType,enterprise:enterprise,province:province,warning_type:warningType,total:total,dt:dt',
        'es.nodes' = '192.168.11.29',
        'es.port' = '9200',
        'es.index.auto.create' = 'TRUE'
        );

-- 创建预警地图统计表，每隔6小时统计所有未审核的数据

CREATE EXTERNAL TABLE province_warning_statistic_es
(
    enterprise   string,
    province     string,
    highrisk_num bigint comment '高风险未审核预警数量',
    medrisk_num  bigint,
    lowrisk_num  bigint,
    safety_num   bigint,
    dt           string
)
    STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
    location '/warningplatform.db/ads/province_warning_statistic_es'
    TBLPROPERTIES ('es.resource' = 'province_warning_index/province_warning',
        'es.mapping.names' =
                'enterprise:enterprise,province:province,highrisk_num:highriskNum,lowrisk_num:lowriskNum,safety_num:safetyNum,dt:dt',
        'es.nodes' = '192.168.11.29',
        'es.port' = '9200'
        );


-- 创建风险等级统计表es映射表，每隔一小时统计所有未审核的数据
CREATE EXTERNAL TABLE risk_level_statistic_es
(
    enterprise   string,
    highrisk_num bigint,
    medrisk_num  bigint,
    lowrisk_num  bigint,
    dt           string
)
    STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
    location '/warningplatform.db/ads/risk_level_statistic_es'
    TBLPROPERTIES ('es.resource' = 'risk_level_index/risk_level',
        'es.mapping.names' = 'enterprise:enterprise,highrisk_num:highriskNum,lowrisk_num:lowriskNum,dt:dt',
        'es.nodes' = '192.168.11.29',
        'es.port' = '9200'
        );


-- 创建预处理之后ods层数据帧原始数据表,按照日期进行分区

create external table ods_preprocess_vehicle_data
(
    data string
) partitioned by (dt string)
    row format delimited fields terminated by '\t'
    location '/warningplatform.db/ods/ods_preprocess_vehicle_data';

-- 创建预处理之后dwd层数据表

create external table dwd_preprocess_vehicle_data
(
    vin                          string,
    msgTime                      string,
    speed                        double,
    startupStatus                string,
    runMode                      string,
    odo                          double,
    gearStatus                   string,
    chargeStatus                 string,
    maxCellVoltageNum            string,
    maxCellVoltage               double,
    minCellVoltageNum            string,
    minCellVoltage               double,
    maxProbeTemperatureNum       string,
    maxProbeTemperature          double,
    minProbeTemperatureNum       string,
    minProbeTemperature          double,
    cellVoltage                  string comment 'double数组',
    differenceCellVoltage        double,
    maxTemperatureRate           double,
    temperatureRate              string,
    atanMaxTemperatureRate       double,
    atanMinTemperatureRate       double,
    averageProbeTemperature      double,
    averageCellVoltage           double,
    varianceCellVoltage          double,
    varianceProbeTemperature     double,
    entropy                      double,
    variation                    double,
    wDifferenceCellVoltages      string comment 'double数组',
    wDifferenceTotalCellVoltage  double,
    differenceInternalResistance double,
    averageModuleCellVoltages    string comment 'double数组',
    maxModuleCellVoltages        string comment 'double数组',
    minModuleCellVoltages        string comment 'double数组',
    maxModuleCellVoltageNums     string comment 'double数组',
    minModuleCellVoltageNums     string comment 'double数组',
    totalModuleCellVoltages      string comment 'double数组',
    differenceModuleCellVoltages string comment 'double数组',
    instantaneousConsumption     double,
    wDischargeRate               double,
    resistance                   double,
    province                     string,
    city                         string,
    country                      string,
    vehicleType                  string,
    enterprise                   string,
    totalCurrent                 double,
    soc                          double
) partitioned by (dt string)
    row format delimited fields terminated by '\t'
        collection items terminated by ','
        map keys terminated by ':'
    location '/warningplatform.db/dwd/dwd_preprocess_vehicle_data';

-- 创建电池包异常数据箱线图es映射表
create external table batterypack_exception_es
(
    enterprise   string,
    province     string,
    vehicle_type string,
    vol_diff_exception
                 struct<Q3 :double,Q2 :double,Q1 :double,maxvalue :double,minvalue :double,vehicles
                        :array<struct<vin:string,outliers:double>>>,
    temper_rate_exception
                 struct<Q3 :double,Q2 :double,Q1 :double,maxvalue :double,minvalue :double,vehicles
                        :array<struct<vin:string,outliers:double>>>,
    temper_exception
                 struct<Q3 :double,Q2 :double,Q1 :double,maxvalue :double,minvalue :double,vehicles
                        :array<struct<vin:string,outliers:double>>>,
    temper_diff_exception
                 struct<Q3 :double,Q2 :double,Q1 :double,maxvalue :double,minvalue :double,vehicles
                        :array<struct<vin:string,outliers:double>>>,
    resistance_exception
                 struct<Q3 :double,Q2 :double,Q1 :double,maxvalue :double,minvalue :double,vehicles
                        :array<struct<vin:string,outliers:double>>>,
    discharge_rate_exception
                 struct<Q3 :double,Q2 :double,Q1 :double,maxvalue :double,minvalue :double,vehicles
                        :array<struct<vin:string,outliers:double>>>,
    dt           string
) row format delimited fields terminated by ','
    collection items terminated by '_'
    map keys terminated by ':'
    STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
    location '/warningplatform.db/ads/batterypack_exception_es'
    TBLPROPERTIES ('es.resource' = 'batterypack_exception/batterypack_exception',
        'es.mapping.names' =
                'enterprise:enterprise,province:province,vehicle_type:vehicleType,vol_diff_exception:cellVoldiffException,temper_rate_exception:temperRateException,temper_exception:temperException,temper_diff_exception:temperDiffException,resistance_exception:resistanceException,discharge_rate_exception:dischargeRateException,Q3:Q3,Q2:Q2,Q1:Q1,maxvalue:maxvalue,minvalue:minvalue,vehicles:vehicles,vin:vin,outliers:outliers',
        'es.nodes' = '192.168.11.29',
        'es.port' = '9200'
        );

-- 创建存储每周计算出来的每辆车的指标均值表
create external table avg_vehicle_data_perweek
(
    enterprise     string,
    province       string,
    vehicleType    string,
    vin            string,
    diff_Voltage   double,
    diff_temper    double,
    temper_rate    double,
    temper         double,
    resistance     double,
    wDischargeRate double
) row format delimited fields terminated by '\t'
    location '/warningplatform.db/dwd/avg_vehicle_data_perweek';

--  创建临时车辆基本信息表,后续需要完善
create external table vehicle_base_info
(
    vin           string,
    delivery_time string comment '出厂时间',
    licensePlate  string
) row format delimited fields terminated by '\t'
    location '/warningplatform.db/dwd/vehicle_base_info';
-- 车辆最初使用时间
create external table vehicle_initial
(
    enterprise string,
    vin        string,
    quarter    string comment '车辆最初使用季度'
) row format delimited fields terminated by '\t'
    location '/warningplatform.db/dwd/vehicle_initial';

--  创建车辆分类表,每月统计一次
create external table vehicle_classification
(
    enterprise     string,
    vin            string,
    classification string
) partitioned by (dt string)
    row format delimited fields terminated by '\t'
    location '/warningplatform.db/dwd/vehicle_classification';

-- 创建预警模型统计es映射表，记录每一周不同车类别的箱线值

CREATE EXTERNAL TABLE warning_boxplot_es
(
    enterprise            string,
    vin                   string,
    chargeMaxVolDiff      double,
    unchargeMaxVolDiff    double,
    chargeMaxTemperRate   double,
    unchargeMaxTemperRate double,
    chargeMaxTemper       double,
    unchargeMaxTemper     double,
    chargeMaxTemperDiff   double,
    unchargeMaxTemperDiff double,
    chargeMinResistance   double,
    unchargeMinResistance double,
    dt                    string
)
    STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
    location '/warningplatform.db/ads/warning_boxplot_es'
    TBLPROPERTIES ('es.resource' = 'warningboxplot/warningboxplot',
        'es.mapping.names' =
                'enterprise:enterprise,vin:vin,chargeMaxVolDiff:chargeMaxVolDiff,unchargeMaxVolDiff:unchargeMaxVolDiff,chargeMaxTemperRate:chargeMaxTemperRate,unchargeMaxTemperRate:unchargeMaxTemperRate,chargeMaxTemper:chargeMaxTemper,unchargeMaxTemper:unchargeMaxTemper,chargeMaxTemperDiff:chargeMaxTemperDiff,unchargeMaxTemperDiff:unchargeMaxTemperDiff,chargeMinResistance:chargeMinResistance,unchargeMinResistance:unchargeMinResistance,dt:dt',
        'es.nodes' = '192.168.11.29',
        'es.port' = '9200'
        );
-- 创建故障信息es映射表
create external table failure_es
(
    enterprise       string,
    vin              string,
    vehicleType      string,
    province         string,
    failureType      string,
    failureStartTime string,
    failureEndTime   string
)
    STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
    location '/warningplatform.db/dwd/failure_es'
    TBLPROPERTIES ('es.resource' = 'failure/failure',
        'es.mapping.names' =
                'enterprise:enterprise,vin:vin,vehicleType:vehicleType,province:province,failureType:failureType,failureStartTime:failureStartTime,failureEndTime:failureEndTime',
        'es.nodes' = '192.168.11.29',
        'es.port' = '9200'
        );

-- 创建故障信息每小时统计es映射表
create external table failure_statistics_perhour_es
(
    enterprise  string,
    province    string,
    vehicleType string,
    vin         string,
    failureType string,
    total       bigint,
    dt          string
)
    STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
    location '/warningplatform.db/ads/failure_statistics_perhour_es'
    TBLPROPERTIES ('es.resource' = 'failure_statistics_perhour/failure_statistics_perhour',
        'es.mapping.names' =
                'enterprise:enterprise,province:province,vehicleType:vehicleType,vin:vin,failureType:failureType,total:total,dt:dt',
        'es.nodes' = '192.168.11.29',
        'es.port' = '9200'
        );

-- 创建故障信息每小时统计表,按照日期分区
create external table failure_statistics_perhour
(
    enterprise  string,
    province    string,
    vehicleType string,
    vin         string,
    failureType string,
    total       bigint,
    dt          string
) partitioned by (day string)
    row format delimited fields terminated by '\t'
    location '/warningplatform.db/ads/failure_statistics_perhour';


-- 创建故障信息每天统计es映射表
create external table failure_statistics_perday_es
(
    enterprise  string,
    province    string,
    vehicleType string,
    vin         string,
    failureType string,
    total       bigint,
    dt          string
)
    STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
    location '/warningplatform.db/ads/failure_statistics_perday_es'
    TBLPROPERTIES ('es.resource' = 'failure_statistics_perday/failure_statistics_perday',
        'es.mapping.names' =
                'enterprise:enterprise,province:province,vehicleType:vehicleType,vin:vin,failureType:failureType,total:total,dt:dt',
        'es.nodes' = '192.168.11.29',
        'es.port' = '9200'
        );
-- 创建初始最高/低电压单体频次表,创建后需要导入ini_vol_cell_frequency_data.txt中的数据
create table ini_vol_cell_frequency
(
    cellVoltageNum string,
    frequency      bigint
) row format delimited fields terminated by '\t'
    location '/warningplatform.db/dwd/ini_vol_cell_frequency';


-- 创建充电记录es映射表
create external table charge_record_es
(
    enterprise             string,
    vehicleType            string,
    province               string,
    vin                    string,
    licensePlate           string,
    chargeStartTime        string,
    chargeEndTime          string,
    chargeStartSoc         double,
    chargeEndSoc           double,
    chargeElectricity      double,
    chargeType             string,
    maxVolCellNumFrequency array<bigint>,
    minVolCellNumFrequency array<bigint>
)
    STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
    location '/warningplatform.db/ads/charge_record_es'
    TBLPROPERTIES ('es.resource' = 'charge_record/charge_record',
        'es.mapping.names' =
                'enterprise:enterprise,vehicleType:vehicleType,province:province,vin:vin,licensePlate:licensePlate,chargeStartTime:chargeStartTime,chargeEndTime:chargeEndTime,chargeStartSoc:chargeStartSoc,chargeEndSoc:chargeEndSoc,chargeElectricity:chargeElectricity,chargeType:chargeType,maxVolCellNumFrequency:maxVolCellNumFrequency,minVolCellNumFrequency:minVolCellNumFrequency',
        'es.nodes' = '192.168.11.29',
        'es.port' = '9200'
        );

-- 创建电流频次分布初始表,创建时要导入ini_current_frequency.txt 中的数据
create table ini_current_frequency
(
    current_rk string,
    frequency  bigint
) row format delimited fields terminated by '\t'
    location '/warningplatform.db/dwd/ini_current_frequency';

-- 创建充电频次es映射表
create external table charge_current_frequency_es
(
    province               string,
    vehicleType            string,
    vin                    string,
    chargeType             string,
    chargeCurrentFrequency array<bigint>,
    statis_time            string
) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
    location '/warningplatform.db/ads/charge_current_frequency_es'
    TBLPROPERTIES ('es.resource' = 'charge_current_frequency/charge_current_frequency',
        'es.mapping.names' =
                'province:province,vehicleType:vehicleType,vin:vin,chargeType:chargeType,chargeCurrentFrequency:chargeCurrentFrequency,statis_time:time',
        'es.nodes' = '192.168.11.29',
        'es.port' = '9200'
        );

-- 创建充电压差扩大模型es映射表

create external table charge_vol_day_diff_es
(
    vin       string,
    dt        string,
    volDiff   array<double>,
    dayDiff   array<double>,
    isWarning string
) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
    location '/warningplatform.db/ads/charge_vol_day_diff_es'
    TBLPROPERTIES ('es.resource' = 'charge_vol_day_diff/charge_vol_day_diff',
        'es.mapping.names' =
                'vin:vin,dt:dt,volDiff:volDiff,dayDiff:dayDiff,isWarning:isWarning',
        'es.nodes' = '192.168.11.29',
        'es.port' = '9200'
        );
