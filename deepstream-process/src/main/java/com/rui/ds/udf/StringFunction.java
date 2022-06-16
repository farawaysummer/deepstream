package com.rui.ds.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

@DeepStreamUDF(name = "to_str")
public class StringFunction extends ScalarFunction {

    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
        return String.valueOf(o);
    }

}
