package com.adc.da.udf;

import com.adc.da.algorithm.PlatformAlgorithm;
import com.adc.da.util.HiveUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.Hive;

import java.util.ArrayList;

/**
 * 单体内阻或者容量差异模型UDF  capacity_anomaly()
 */
public class CapacityAnomalyUdf extends UDF {
    public String evaluate(ArrayList<String> chargeVol, ArrayList<String> disChargeVol, int th1, double th2, double th3) {

        // 充电电压
        double[][] cvols = HiveUtils.parseVol(chargeVol);

        // 放电电压
        double[][] dvols = HiveUtils.parseVol(disChargeVol);

        return new PlatformAlgorithm().capacityAbnormal(cvols, dvols, th1, th2, th3) + "";
    }
}
