package com.adc.da.bean;

import lombok.Data;

/**
 * 经过预处理后的bean对象，只封装了监控充电/行驶完成需要的字段信息
 */
@Data
public class OdsData {
    private String vin;
    private Long msgTime;
    private double speed;

    private String startupStatus;  // 启动状态
    private String gearStatus;     // 档位
    private String chargeStatus;   // 充电状态
    private double soc;



}
