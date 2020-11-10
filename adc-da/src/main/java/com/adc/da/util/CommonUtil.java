package com.adc.da.util;


import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 通用工具类
 */
public class CommonUtil {

    /**
     * byte[]转int
     *
     * @param bytes
     * @return
     */
    public static int byteArrayToInt(byte[] bytes) {
        int value = 0;
        // 由高位到低位
        for (int i = 0; i < 4; i++) {
            int shift = (4 - 1 - i) * 8;
            value += (bytes[i] & 0x000000FF) << shift;// 往高位游
        }
        return value;
    }

    // 加载配置文件
    public static Properties loadProperties(String configPath) {
        Properties properties = new Properties();
        InputStream in = null;
        try {
            in = CommonUtil.class.getClassLoader().getResourceAsStream(configPath);
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return properties;
    }

    // 初始化执行环境
    public static StreamExecutionEnvironment initEnvironment() throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  设置状态后端与检查点

        env.setStateBackend(new MemoryStateBackend());
        //RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs://192.168.11.32:8020/flink-checkpoints", true);
        //rocksDBStateBackend.setDbStoragePath("file:///home/flink/rocksdb");

        //StateBackend stateBackend = rocksDBStateBackend;
        //env.setStateBackend(stateBackend);

        // 触发检查点的间隔，周期性启动检查点，单位ms
        env.enableCheckpointing(1000L);
        //设置状态一致性级别
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500L);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));
        return env;
    }

}
