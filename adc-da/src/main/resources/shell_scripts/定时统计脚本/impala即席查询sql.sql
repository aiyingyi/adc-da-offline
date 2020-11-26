
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