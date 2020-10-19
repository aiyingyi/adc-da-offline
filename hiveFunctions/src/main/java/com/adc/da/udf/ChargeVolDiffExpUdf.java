package com.adc.da.udf;

import com.adc.da.algorithm.PlatformAlgorithm;
import com.adc.da.util.HiveUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

/**
 * 充电压差扩大模型算法自定义UDF函数
 * hive函数名：charge_vol_diff_exp()
 *
 * @author aiyingyi
 * @date 2020/10/09
 */
public class ChargeVolDiffExpUdf extends UDF {
    /**
     * 传入的列是数组类型，使用List去接收，而不是数组
     *
     * @param volDiff 充电压差数组
     * @param time 充电时间
     * @return 0/1 表示是否发生预警
     */
    public String evaluate(ArrayList<Double> volDiff, ArrayList<String> time) {
        if (volDiff == null || time == null) {
            return null;
        }
        return new PlatformAlgorithm().chargeDifferentialVoltageExpansion(HiveUtils.listToArray(volDiff), (String[])time.toArray()) + "";
    }
}
