package com.adc.da.udf;

import com.adc.da.algorithm.PlatformAlgorithm;
import com.adc.da.util.HiveUtils;

import java.util.ArrayList;

/**
 * 绝缘电阻突降hive自定义UDF函数：r_reduce()
 */
public class ResistanceRreduceUdf {

    /**
     * @param res 电阻值
     * @param th  阈值
     * @param win 平滑窗口
     * @return  是否产生预警
     */
    public String evaluate(ArrayList<Double> res,int th,int win) {
        if(res == null || res.size()==0)
            return null;
        return new PlatformAlgorithm().rReduce(HiveUtils.listToArray(res),th,win) + "";
    }
}
