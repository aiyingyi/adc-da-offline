package com.adc.da.util;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public class HiveUtils {

    // 将List转换成array
    public static double[] listToArray(List<Double> list) {

        double arr[] = new double[list.size()];
        for (int i = 0; i < list.size(); i++) {
            arr[i] =  list.get(i);
        }
        return arr;
    }
}
