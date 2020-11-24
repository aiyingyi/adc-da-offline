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





--------------------------------------------------------最新版  计算充电电量累加

-- 刷新元数据，具体使用哪一种方式还需要验证
INVALIDATE METADATA warningplatform.ods_preprocess_vehicle_data;

refresh warningplatform.ods_preprocess_vehicle_data;

WITH charge_data AS
         (SELECT get_json_object(`data`, '$.vin') vin,
                 unix_timestamp(get_json_object(`data`, '$.msgTime')) msgTime,
                 cast(get_json_object(`data`, '$.totalCurrent') AS DOUBLE) totalCurrent,
                 get_json_object(`data`, '$.chargeStatus') chargeStatus
          FROM warningplatform.ods_preprocess_vehicle_data
          WHERE dt >= from_unixtime(unix_timestamp('${startTime}'), 'yyyy-MM-dd')
            AND dt <= from_unixtime(unix_timestamp('${endTime}'), 'yyyy-MM-dd')
            AND from_unixtime(unix_timestamp(get_json_object(`data`, '$.msgTime')), 'yyyy-MM-dd HH:mm:ss') >= '${startTime}'
            AND from_unixtime(unix_timestamp(get_json_object(`data`, '$.msgTime')), 'yyyy-MM-dd HH:mm:ss') <= '${endTime}' ), -- 计算每一帧的充电电量
     charge_ele as
         (
             select
                 chargeInfo.vin,
                 chargeInfo.msgTime,
                 sum(chargeInfo.timeGap *chargeInfo.totalCurrent ) over (partition BY vin   ORDER BY msgTime)


             from (
                      SELECT tmp.vin,
                             tmp.msgTime,
                             CASE
                                 WHEN (tmp.msgTime - tmp.lastMsgTime) > 20 THEN 0
                                 ELSE (tmp.msgTime - tmp.lastMsgTime)/3600
                                 END AS timeGap,
                             tmp.totalCurrent
                      FROM
                          (SELECT vin,
                                  totalCurrent,
                                  msgTime,
                                  lag(msgTime, 1) over (partition BY vin
                                      ORDER BY msgTime ASC) AS lastMsgTime,
                                  chargeStatus
                           FROM charge_data) AS tmp
                      WHERE tmp.lastMsgTime IS NOT NULL
                        AND tmp.chargeStatus = '1') as chargeInfo -- 计算充电电量
         )

select * from charge_ele