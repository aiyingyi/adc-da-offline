package com.adc.da.util

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FlinkUtil {

  // 设置执行环境
  def initEnvironment:StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(10)
    //  设置状态后端与检查点
    env.setStateBackend(new MemoryStateBackend())
    // 触发检查点的间隔，周期性启动检查点，单位ms
    env.enableCheckpointing(1000L)
    //设置状态一致性级别
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000L)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L))
    env

  }

}
