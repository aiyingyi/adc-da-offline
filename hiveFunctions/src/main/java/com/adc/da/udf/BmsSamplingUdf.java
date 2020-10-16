package com.adc.da.udf;

import com.adc.da.algorithm.PlatformAlgorithm;
import com.adc.da.util.HiveUtils;
import jodd.util.ArraysUtil;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.function.ToIntFunction;

/**
 * 连接阻抗大模型算法udf函数
 * 自定义函数 bms_sampling()
 */
public class BmsSamplingUdf extends UDF {

    public String evaluate(ArrayList<Integer> maxNum, ArrayList<Integer> minNum, double vdet, int th1, int th2) {

        int[] max = ArrayUtils.toPrimitive((Integer[]) maxNum.toArray());
        int[] min = ArrayUtils.toPrimitive((Integer[]) minNum.toArray());

        return new PlatformAlgorithm().bmsSamplingAnomaly(vdet, max, min, th1, th2) + "";
    }
}
