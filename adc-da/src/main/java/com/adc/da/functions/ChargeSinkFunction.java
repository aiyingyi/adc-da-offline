package com.adc.da.functions;

import ch.ethz.ssh2.Connection;
import com.adc.da.bean.ChargeRecord;
import com.adc.da.util.ShellUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import scala.actors.threadpool.Arrays;

import java.util.Properties;

/**
 * 1. 充电压差扩大模型算法调用
 * 2. 充电完成后调用:1. 电池包衰减预警模型,2. 执行充电方式，电量以及最大最低电压单体频次脚本
 */

public class ChargeSinkFunction extends RichSinkFunction<ChargeRecord> {

    // 充电压差扩大模型的充电次数窗口大小
    private int windowSize = 0;

    // shell环境配置以及脚本执行路径
    private Properties shellConfig = null;

    public ChargeSinkFunction(int windowSize, Properties shellConfig) {
        this.windowSize = windowSize;
        this.shellConfig = shellConfig;
    }

    // 充电次数
    ValueState<Integer> chargeTimes = null;
    // 记录符合条件的10次充电起始和结束时间
    ValueState<long[]> chargeStartAndEnd = null;
    Connection conn = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        chargeTimes = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("chargeTimes", Integer.class));
        chargeStartAndEnd = getRuntimeContext().getState(new ValueStateDescriptor("chargeStartAndEnd", long[].class));
        conn = ShellUtil.getConnection(shellConfig.getProperty("userName"), shellConfig.getProperty("passWord"), shellConfig.getProperty("ip"), Integer.parseInt(shellConfig.getProperty("port")));
    }

    @Override
    public void close() throws Exception {
        chargeTimes.clear();
        chargeStartAndEnd.clear();
    }

    @Override
    public void invoke(ChargeRecord value, SinkFunction.Context context) throws Exception {


        // 1. 执行充电方式，电量以及最大最低电压单体频次脚本
        ShellUtil.exec(conn, shellConfig.getProperty("chargeStyleElectricityFrequencyPath") + " " + value.getStartTime() + " " + value.getEndTime() + " " + value.getVin());

        // 2. TODO 电池包衰减预警模型
        if (value.getEndSoc() - value.getStartSoc() > 40) {
            // ShellUtil.exec(conn) );
        }
        // 3.充电压差扩大模型
        if (value.getStartSoc() <= 80 && value.getEndSoc() >= 80) {

            if (chargeTimes.value() == null) {
                // 初始化状态，不可以在open()中初始化
                chargeTimes.update(1);
                chargeStartAndEnd.update(new long[2 * windowSize]);
            } else {
                chargeTimes.update(chargeTimes.value() + 1);
            }
            // 获取之前10次的充电时间
            long[] arr = chargeStartAndEnd.value();

            // 相当于一个循环队列，
            int index = (chargeTimes.value() % windowSize + 1) * 2 - 1;
            arr[index - 1] = value.getStartTime();
            arr[index] = value.getEndTime();
            // 状态更新
            chargeStartAndEnd.update(arr);
            // 假如充电次数超过了windowSize次
            if (chargeTimes.value() >= windowSize) {
                long[] timeArray = chargeStartAndEnd.value();
                // 时间数组按照值进行排序，是对充电进行排序
                Arrays.sort(timeArray);

                // 拼接10次充电的时间戳字符串
                String shellArgs = " ";
                for (long time : timeArray) {
                    shellArgs = shellArgs + " " + time + " ";
                }
                shellArgs = shellArgs + value.getVin();
                // 传入执行脚本的路径和时间参数,以及vin码
                ShellUtil.exec(conn, shellConfig.getProperty("chargeVolDiffExtendModulePath") + shellArgs);
            }
        }
    }
}

