package com.rui.ds.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Objects;

@DeepStreamUDF(name = "DP_DECODE")
public class DecodeFunction extends ScalarFunction {

    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object condition, @DataTypeHint(inputGroup = InputGroup.ANY) Object... values) {
        if (values.length % 2 != 0) {
            return String.valueOf(condition);
        }

        for (int index = 0 ; index < values.length ; index = index + 2) {
            if (Objects.equals(condition, values[index])) {
                return String.valueOf(values[index + 1]);
            }
        }

        return "";
    }

}