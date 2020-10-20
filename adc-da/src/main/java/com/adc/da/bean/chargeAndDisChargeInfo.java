package com.adc.da.bean;

import lombok.Data;

/**
 * 充电记录
 */

/**
 * 充放电时间信息
 */
@Data
public class chargeAndDisChargeInfo {

    private String vin;
    private long chargeStartTime;
    private long chargeEndTime;
    private long disChargeStartTime;
    private long disChargeEndTime;

    public chargeAndDisChargeInfo() {

    }

    @Override
    public String toString() {
        return "chargeAndDisChargeInfo{" +
                "vin='" + vin + '\'' +
                ", chargeStartTime=" + chargeStartTime +
                ", chargeEndTime=" + chargeEndTime +
                ", disChargeStartTime=" + disChargeStartTime +
                ", disChargeEndTime=" + disChargeEndTime +
                '}';
    }
}
