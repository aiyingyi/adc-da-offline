package com.adc.da.udf;

import com.adc.da.algorithm.PlatformAlgorithm;
import org.apache.hadoop.hive.ql.exec.UDF;

public class TestUdf extends UDF {
    public String evaluate(String str) {

        if (str==null) {
            return null;
        }

        return str+"hhhhhh";

    }
}
