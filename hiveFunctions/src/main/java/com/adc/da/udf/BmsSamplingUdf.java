package com.adc.da.udf;

import com.adc.da.algorithm.PlatformAlgorithm;

import org.apache.hadoop.hive.ql.exec.UDF;


import java.util.ArrayList;


/**
 * 连接阻抗大模型算法udf函数
 * 自定义函数 bms_sampling()
 */
public class BmsSamplingUdf extends UDF {

    public String evaluate(ArrayList<Integer> maxNum, ArrayList<Integer> minNum, double vdet, int th1, int th2) {

        int[] max = new int[maxNum.size()];
        for (int i = 0; i < maxNum.size(); i++) {
            max[i] = maxNum.get(i).intValue();
        }

        int[] min = new int[minNum.size()];
        for (int i = 0; i < minNum.size(); i++) {
            min[i] = minNum.get(i).intValue();
        }

        return new PlatformAlgorithm().bmsSamplingAnomaly(vdet, max, min, th1, th2) + "";
    }
}
