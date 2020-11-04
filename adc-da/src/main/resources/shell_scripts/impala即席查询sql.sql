------------------------------------------------------计算累加充电电量
-- 刷新元数据，具体使用哪一种方式还需要验证
INVALIDATE METADATA warningplatform.ods_preprocess_vehicle_data;
refresh warningplatform.ods_preprocess_vehicle_data;


with charge_data as
         (
             select get_json_object(`data`, '$.vin')                          vin,
                    cast(get_json_object(`data`, '$.msgTime') as bigint)      msgTime,
                    cast(get_json_object(`data`, '$.totalCurrent') as double) totalCurrent,
                    get_json_object(`data`, '$.chargeStatus')                 chargeStatus
             from warningplatform.ods_preprocess_vehicle_data
             where dt >= from_unixtime(unix_timestamp('${startTime}'), 'yyyy-MM-dd')
               and dt <= from_unixtime(unix_timestamp('${endTime}'), 'yyyy-MM-dd')
               and from_unixtime(cast(cast(get_json_object(`data`, '$.msgTime') as bigint) / 1000 as bigint),
                                 'yyyy-MM-dd HH:mm:ss') >= '${startTime}'
               and from_unixtime(cast(cast(get_json_object(`data`, '$.msgTime') as bigint) / 1000 as bigint),
                                 'yyyy-MM-dd HH:mm:ss') <= '${endTime}'
         ),
-- 计算每一帧的充电电量
     charge_ele as
         (
             select tmp.vin,
                    tmp.msgTime,
                    (tmp.msgTime - tmp.lastMsgTime) / (1000 * 3600) * tmp.totalCurrent as ele
             from (
                      select vin,
                             totalCurrent,
                             chargeStatus,
                             msgTime,
                             lag(msgTime, 1) over (partition by vin order by msgTime asc ) as lastMsgTime
                      from charge_data
                  ) as tmp
             where tmp.lastMsgTime is not null
               and tmp.chargeStatus != '0'
         )
--  变量累加
select ce.vin,
       ce.msgTime,
       round(ce.accEle, 1) as accEle
from (
         select vin,
                msgTime,
                sum(ele) over (partition by vin order by msgTime) as accEle
         from charge_ele
     ) as ce