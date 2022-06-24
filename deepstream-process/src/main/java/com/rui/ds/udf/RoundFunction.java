package com.rui.ds.udf;

import com.rui.ds.common.DeepStreamUDF;
import org.apache.flink.table.functions.ScalarFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;

@DeepStreamUDF("ROUND")
public class RoundFunction extends ScalarFunction {

    public Double eval(Double value,
                       Integer numDig) {

        BigDecimal b = new BigDecimal(Double.toString(value));

        BigDecimal one = new BigDecimal("1");

        return b.divide(one, numDig, RoundingMode.HALF_UP).doubleValue();
    }

}