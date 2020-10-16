package com.adc.da.udf;

import com.adc.da.algorithm.PlatformAlgorithm;
import com.adc.da.util.HiveUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

/**
 * 自定义UDF函数，vol_fluctuation()，单体电压波动性差异大
 */
public class VolFluctuationUdf extends UDF {

    public String evaluate(ArrayList<Double> maxVol, ArrayList<Double> minVol, double th) {
        double[] max = HiveUtils.listToArray(maxVol);
        double[] min = HiveUtils.listToArray(minVol);
        return new PlatformAlgorithm().vJumpConsist(max, min, th) + "";
    }
}
