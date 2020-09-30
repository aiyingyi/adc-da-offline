package com.adc.da.udf;

import com.adc.da.algorithm.PlatformAlgorithm;
import org.apache.hadoop.hive.ql.exec.UDF;

public class ChargeVolDiffExpUdf extends UDF {

    public String evaluate(double volDiff[], double dayDiff[]) {

        if (volDiff == null || dayDiff == null) {
            return null;
        }

        return new PlatformAlgorithm().chargeDifferentialVoltageExpansion(volDiff, dayDiff) + "";
    }

}
