package com.adc.da.udf;

import com.adc.da.algorithm.PlatformAlgorithm;
import com.adc.da.util.HiveUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

/**
 * 连接阻抗大模型算法udf函数
 * 自定义函数 conn_impedance()
 */
public class ConnectionImpedanceUdf extends UDF {

    public String evaluate(ArrayList<Double> vdet, ArrayList<Double> I, ArrayList<Double> soc, double rth1, double rth2) {

        double[] vdet0 = HiveUtils.listToArray(vdet);
        double[] I0 = HiveUtils.listToArray(I);
        double[] soc0 = HiveUtils.listToArray(soc);
        return new PlatformAlgorithm().highConnectionImpedance(vdet0, I0, soc0, rth1, rth2) + "";

    }
}
