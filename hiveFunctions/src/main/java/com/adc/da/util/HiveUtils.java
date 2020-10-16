package com.adc.da.util;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HiveUtils {

    // 将List转换成array
    public static double[] listToArray(List<Double> list) {

        double arr[] = new double[list.size()];
        for (int i = 0; i < list.size(); i++) {
            arr[i] = list.get(i);
        }
        return arr;
    }

    /**
     * 将字符串解析成二维的double数组
     *
     * @param cellVol
     * @return
     */
    public static double[][] parseVol(ArrayList<String> cellVol) {

        double[][] vols = new double[cellVol.size()][96];
        // 将字符串解析成数组
        for (int i = 0; i < cellVol.size(); i++) {
            String[] strs = cellVol.get(i).substring(1, cellVol.get(i).length() - 1).split(",");
            Stream<Double> doubleStream = Arrays.stream(strs).map(new Function<String, Double>() {
                @Override
                public Double apply(String s) {
                    return new Double(s);
                }
            });
            double[] voltage = HiveUtils.listToArray(doubleStream.collect(Collectors.toList()));
            vols[i] = voltage;
        }
        return vols;

    }
}
