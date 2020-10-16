package com.adc.da.bean;

import lombok.Data;

/**
 * 充电记录
 */
@Data
public class ChargeRecord {

    private String vin;
    private long startTime;
    private long endTime;
    private double startSoc;
    private double endSoc;

    public ChargeRecord(String vin, long startTime, long endTime, double startSoc, double endSoc) {
        this.vin = vin;
        this.startTime = startTime;
        this.endTime = endTime;
        this.startSoc = startSoc;
        this.endSoc = endSoc;
    }

    @Override
    public String toString() {
        return "ChargeRecord{" +
                "vin='" + vin + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", startSoc=" + startSoc +
                ", endSoc=" + endSoc +
                '}';
    }
}
