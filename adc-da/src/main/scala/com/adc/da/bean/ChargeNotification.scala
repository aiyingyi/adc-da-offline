package com.adc.da.bean

/**
 * 充电开始和结束样例类
 * @param endMsgTime
 * @param startMsgTime
 * @param vin
 */
case class ChargeNotification(endMsgTime: String,startMsgTime: String,vin: String)
