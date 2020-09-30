package com.adc.da.function

import java.io.InputStream
import java.util.Properties

import ch.ethz.ssh2.Connection
import com.adc.da.bean.ChargeNotification
import com.adc.da.util.{FlinkUtil, ShellUtil}
import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala._

/**
 * 后续将所有配置写进配置文件
 *
 * @author aiyingyi
 * @date 2020/09/18
 */
object ChargeStyleElectricityFrequency {
  // 从kafka中获取充电信息
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = FlinkUtil.initEnvironment

    // 配置kafka属性
    val kafkaProperties = new Properties()

    // 远程读取服务器
    val shellProperties = new Properties()

    // 加载kafka 配置文件
    val kafkaIn: InputStream = ChargeStyleElectricityFrequency.getClass.getClassLoader.getResourceAsStream("config/chargeNotification.properties")
    kafkaProperties.load(kafkaIn)
    val shellIn: InputStream = ChargeStyleElectricityFrequency.getClass.getClassLoader.getResourceAsStream("config/shell.properties")
    shellProperties.load(shellIn)
    // 关闭流
    if (kafkaIn != null) {
      kafkaIn.close()
    } else if (shellIn != null) {
      shellIn.close()
    }

    // 创建数据源
    val dataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String](kafkaProperties.getProperty("topic"), new SimpleStringSchema(), kafkaProperties))

    // 将消息转换成样例类
    val notation: DataStream[ChargeNotification] = dataStream
      .map(data => JSON.parseObject[ChargeNotification](data, classOf[ChargeNotification]))
    // 按照vin进行keyBy
    notation.keyBy(_.vin).addSink(new RichSinkFunction[ChargeNotification] {

      var conn: Connection = _

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)

        conn = ShellUtil.getConnection(shellProperties.getProperty("userName"), shellProperties.getProperty("passWord"), shellProperties.getProperty("ip"), shellProperties.getProperty("port").toInt)
      }

      // 执行脚本，kafka发送来的一条数据都会执行一次
      override def invoke(chargeNotation: ChargeNotification, context: SinkFunction.Context[_]): Unit = {
        ShellUtil.exec(conn, shellProperties.getProperty("chargeStyleElectricityFrequencyPath") + " " + chargeNotation.startMsgTime + " " + chargeNotation.endMsgTime + " " + chargeNotation.vin)
      }

      // 关闭连接
      override def close(): Unit = {
        super.close()
        conn.close()
      }
    })
    // 执行任务
    env.execute("charge_mode_electricity_frequency")

  }

}
