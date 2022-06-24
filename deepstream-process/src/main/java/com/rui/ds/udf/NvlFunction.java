package com.rui.ds.udf;

import com.rui.ds.common.DeepStreamUDF;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

@DeepStreamUDF("NVL")
public class NvlFunction extends ScalarFunction {

    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object first, String second) {
        if (first == null) {
            return second;
        } else {
            return first.toString();
        }
    }

}