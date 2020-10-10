package com.adc.da.function

import java.io.InputStream
import java.util.Properties

import ch.ethz.ssh2.Connection
import com.adc.da.bean.{ChargeInfo, ChargeRecord}
import com.adc.da.util.{FlinkUtil, ShellUtil}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

/**
 * 实时监控车辆充电数据，将符合条件的充电过程筛选出来,执行充电压差扩大模型算法脚本/或者电池包衰减预警模型
 */
object ChargeMonitor {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = FlinkUtil.initEnvironment
    env.setParallelism(1)

    // 设置触发计算的充电次数的大小
    val windowSize = 10
    // 配置kafka属性
    val kafkaProperties = new Properties()
    // 服务器登录配置
    val shellProperties = new Properties()

    // 加载kafka 配置文件
    val kafkaIn: InputStream = ChargeStyleElectricityFrequency.getClass.getClassLoader.getResourceAsStream("config/chargeMonitor.properties")
    kafkaProperties.load(kafkaIn)

    val shellIn: InputStream = ChargeStyleElectricityFrequency.getClass.getClassLoader.getResourceAsStream("config/shell.properties")
    shellProperties.load(shellIn)

    if (kafkaIn != null) {
      kafkaIn.close()
    } else if (shellIn != null) {
      shellIn.close()
    }


    // 创建数据源
    val dataStream: DataStream[ChargeInfo] = env.addSource(new FlinkKafkaConsumer011[String](kafkaProperties.getProperty("topic"), new SimpleStringSchema(), kafkaProperties))
      .map(data => {
        val obj = JSON.parseObject(data, classOf[JSONObject])
        val soc = obj.getString("soc").substring(0, obj.getString("soc").length - 1).toDouble
        ChargeInfo(obj.getString("vin"), obj.getString("chargeStatus"), soc, obj.getString("msgTime"))
      })

    // 充电压差扩大模型算法
    dataStream.keyBy(data => data.vin).process(ChargeProcessFunction(windowSize)).addSink(new RichSinkFunction[ChargeRecord] {
      var conn: Connection = _

      //  初始化ssh连接
      override def open(parameters: Configuration): Unit = {
        conn = ShellUtil.getConnection(shellProperties.getProperty("userName"), shellProperties.getProperty("passWord"), shellProperties.getProperty("ip"), shellProperties.getProperty("port").toInt)
      }

      // 在此处执行shell脚本
      override def invoke(value: ChargeRecord, context: SinkFunction.Context[_]): Unit = {
        // 拼接10次充电的时间戳字符串
        var shellArgs = " "
        for (elem <- value.record) {
          shellArgs = shellArgs + " " + elem + " "
        }
        shellArgs = shellArgs + value.vin
        // 传入执行脚本的路径和时间参数,以及vin码
        ShellUtil.exec(conn, shellProperties.getProperty("chargeVolDiffExtendModulePath") + shellArgs)
      }

      override def close(): Unit = {
        super.close()
        if (conn != null) {
          conn.close()
        }
      }
    })

    // 监控充电状态，计算充电容量
    dataStream.keyBy(data => data.vin).process(new KeyedProcessFunction[String, ChargeInfo, (String, String, String, Double, Double)] {
      // 保存是否充电状态
      private var isCharge: ValueState[Boolean] = _
      private var startTime: ValueState[String] = _
      private var endTime: ValueState[String] = _
      private var startSoc: ValueState[Double] = _
      private var endSoc: ValueState[Double] = _

      override def open(parameters: Configuration): Unit = {

        super.open(parameters)
        isCharge = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isCharge", classOf[Boolean]))

        startTime = getRuntimeContext.getState(new ValueStateDescriptor[String]("startTime", classOf[String]))
        endTime = getRuntimeContext.getState(new ValueStateDescriptor[String]("endTime", classOf[String]))
        // 充电电量
        startSoc = getRuntimeContext.getState(new ValueStateDescriptor[Double]("startSoc", classOf[Double]))
        endSoc = getRuntimeContext.getState(new ValueStateDescriptor[Double]("endSoc", classOf[Double]))
      }

      // (String, String, String)为vin，startTime，endTime
      override def processElement(value: ChargeInfo, ctx: KeyedProcessFunction[String, ChargeInfo, (String, String, String, Double, Double)]#Context, out: Collector[(String, String, String, Double, Double)]): Unit = {
        val vin: String = value.vin
        val chargeStatus: String = value.chargeStatus
        val msgTime: String = value.msgTime
        val soc: Double = value.soc

        if ("1".equals(chargeStatus)) {
          // 如果开始充电
          if (isCharge.value() == null || isCharge.value() == false) {
            // 更新状态
            isCharge.update(true)
            startSoc.update(soc)
            endSoc.update(soc)
            startTime.update(msgTime)
            endTime.update(msgTime)
          } else { // 如果已经处于充电状态
            if (startTime.value() > msgTime)
              startTime.update(msgTime)
            if (endTime.value() < msgTime)
              endTime.update(msgTime)
            if (startSoc.value() > soc)
              startSoc.update(soc)
            if (endSoc.value() < soc)
              endSoc.update(soc)
          }
        } else { // 假如不处于充电模式
          if (isCharge.value() == true && startTime.value() < value.msgTime) { // 之前处于充电状态
            isCharge.update(false)
            if (endSoc.value() - startSoc.value() > 40) {
              out.collect((vin, startTime.value(), endTime.value(), startSoc.value(), endSoc.value()))
            }
          }
        }
      }
    }).addSink(new RichSinkFunction[(String, String, String, Double, Double)] {
      var conn: Connection = _

      //  初始化ssh连接
      override def open(parameters: Configuration): Unit = {
        conn = ShellUtil.getConnection(shellProperties.getProperty("userName"), shellProperties.getProperty("passWord"), shellProperties.getProperty("ip"), shellProperties.getProperty("port").toInt)
      }

      // 在此处执行shell脚本
      override def invoke(value: (String, String, String, Double, Double), context: SinkFunction.Context[_]): Unit = {
        ShellUtil.exec(conn, shellProperties.getProperty("chargeCapacityPath") + " " + value._1 + " " + value._2 + " " + value._3 + " " + value._4 + " " + value._5)
      }

      override def close(): Unit = {
        super.close()
        if (conn != null) {
          conn.close()
        }
      }
    })
    // 执行任务
    env.execute("chargeMonitor")
  }
}

/**
 * 监控10次有效充电的处理函数
 *
 * @param windowSize 窗口大小
 */
case class ChargeProcessFunction(windowSize: Int) extends KeyedProcessFunction[String, ChargeInfo, ChargeRecord] {
  // 充电次数
  private var chargeTimes: ValueState[Long] = _
  // 保存是否充电状态
  private var isCharge: ValueState[Boolean] = _
  // 记录符合条件的10次充电信息
  private var chargeStartAndEnd: ValueState[Array[String]] = _

  // 记录充电结束开始时间
  private var startTime: ValueState[String] = _
  private var endTime: ValueState[String] = _

  private var startSoc: ValueState[Double] = _
  private var endSoc: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {

    super.open(parameters)
    chargeTimes = getRuntimeContext.getState(new ValueStateDescriptor[Long]("chargeTimes", classOf[Long]))
    isCharge = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isCharge", classOf[Boolean]))

    // 记录符合条件的10次充电信息
    chargeStartAndEnd = getRuntimeContext.getState[Array[String]](new ValueStateDescriptor[Array[String]]("chargeStartAndEnd", classOf[Array[String]]))

    startTime = getRuntimeContext.getState(new ValueStateDescriptor[String]("startTime", classOf[String]))
    endTime = getRuntimeContext.getState(new ValueStateDescriptor[String]("endTime", classOf[String]))

    // 充电电量
    startSoc = getRuntimeContext.getState(new ValueStateDescriptor[Double]("startSoc", classOf[Double]))
    endSoc = getRuntimeContext.getState(new ValueStateDescriptor[Double]("endSoc", classOf[Double]))
  }

  override def processElement(value: ChargeInfo, ctx: KeyedProcessFunction[String, ChargeInfo, ChargeRecord]#Context, out: Collector[ChargeRecord]): Unit = {

    val vin: String = value.vin
    val chargeStatus: String = value.chargeStatus
    val msgTime: String = value.msgTime
    val soc: Double = value.soc

    if ("1".equals(chargeStatus)) {
      // 如果开始充电
      if (isCharge.value() == null || isCharge.value() == false) {
        // 更新状态
        isCharge.update(true)
        startSoc.update(soc)
        endSoc.update(soc)
        startTime.update(msgTime)
        endTime.update(msgTime)
      } else { // 如果已经处于充电状态
        if (startTime.value() > msgTime)
          startTime.update(msgTime)
        if (endTime.value() < msgTime)
          endTime.update(msgTime)
        if (startSoc.value() > soc)
          startSoc.update(soc)
        if (endSoc.value() < soc)
          endSoc.update(soc)
      }
    } else { // 假如不处于充电模式
      if (isCharge.value() == true && startTime.value() < value.msgTime) { // 之前处于充电状态
        isCharge.update(false)
        // 只有满足条件的充电才会被加入计算
        if (startSoc.value() < 80 && endSoc.value() >= 80) {
          // 充电次数加1
          if (chargeTimes.value() == null) {
            chargeTimes.update(1)
            chargeStartAndEnd.update(new Array[String](2 * windowSize))
          } else {
            chargeTimes.update(chargeTimes.value() + 1)
          }
          val arr: Array[String] = chargeStartAndEnd.value()
          val index = ((chargeTimes.value() % windowSize + 1) * 2 - 1).toInt
          // 更新新一轮的充电开始和结束时间
          arr(index - 1) = startTime.value()
          arr(index) = endTime.value()
          // 更新状态
          chargeStartAndEnd.update(arr)
          if (chargeTimes.value() >= windowSize) {
            val timeArray: Array[String] = chargeStartAndEnd.value()
            // 时间数组按照值进行排序，是对充电进行排序
            val array: Array[String] = timeArray.sortBy(elem => elem)
            chargeStartAndEnd.update(array)
            out.collect(ChargeRecord(vin, chargeStartAndEnd.value()))
          }
        }
      }
    }
  }
}

