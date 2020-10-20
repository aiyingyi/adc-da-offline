package com.adc.da.bean;

import lombok.Data;

/**
 * 保存事件的开始和结束时间
 */
@Data
public class EventInfo {
    private String vin;
    private long startTime;
    private long endTime;


    public EventInfo(String vin, long startTime, long endTime) {
        this.vin = vin;
        this.startTime = startTime;
        this.endTime = endTime;
    }
}
