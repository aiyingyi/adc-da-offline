package com.adc.da.bean

/**
 * 充电压差模型扩大算法，record为10次充电的开始时间和结束时间
 * @param vin
 * @param record
 */
case class ChargeRecord(vin:String,record:Array[String])
