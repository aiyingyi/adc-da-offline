package com.adc.da.udf;

import com.adc.da.algorithm.PlatformAlgorithm;
import com.adc.da.util.HiveUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;


/**
 * 单体电压离散度高模型算法
 * cell_vol_highdis()
 */
public class CellVolHighDisUdf extends UDF {
    public String evaluate(ArrayList<String> cellVol, int th1, int th2) {

        double[][] vols = HiveUtils.parseVol(cellVol);
        // 执行算法调用
        return new PlatformAlgorithm().highDisPersionEarlyWarning(vols, th1, th2) + "";
    }
}
