package com.adc.da.udf;

import com.adc.da.util.HiveUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 单体电压离散度高模型算法
 */
public class CellVolHighDisUdf extends UDF {
    public String evaluate(String[] cellVol, int th1, int th2) {
        double[][] vols = new double[cellVol.length][96];
        // 将字符串解析成数组
        for (int i = 0; i < cellVol.length; i++) {
            String[] strs = cellVol[i].substring(1, cellVol[i].length() - 1).split(",");
            Stream<Double> doubleStream = Arrays.stream(strs).map(new Function<String, Double>() {
                @Override
                public Double apply(String s) {
                    return new Double(s);
                }
            });
            double[] voltage = HiveUtils.listToArray(doubleStream.collect(Collectors.toList()));
            vols[i] = voltage;
        }

        // 执行算法调用
        return null;
    }
}
