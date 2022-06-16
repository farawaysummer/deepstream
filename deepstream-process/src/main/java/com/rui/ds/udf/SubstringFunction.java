package com.rui.ds.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class SubstringFunction extends ScalarFunction {

    public String eval(String s, Integer begin, Integer end) {
        return s.substring(begin, end);
    }

}