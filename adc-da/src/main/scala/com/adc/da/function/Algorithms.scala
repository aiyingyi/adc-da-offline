package com.adc.da.function

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object Algorithms {
  /**
   * 充电压差扩大模型算法
   *
   * @param vin
   * @param chargeTime 数组，元素2*n-1,2*n-2分别是第n次充电的开始和结束时间
   */
  def chargeVolDiffExtendModule(vin: String, chargeTime: Array[String]): Unit = {


  }

  /**
   * 获取批处理Table环境
   *
   * @return TableEnvironment
   */
  def getBatchTableEnv: TableEnvironment = {
    val bbSettings: EnvironmentSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inBatchMode.build
    TableEnvironment.create(bbSettings)
  }

  def main(args: Array[String]): Unit = {


  }
}
