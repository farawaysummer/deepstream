package com.rui.ds.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

@DeepStreamUDF(name = "DP_CONCAT")
public class ConcatFunction extends ScalarFunction {

    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object first,
                       @DataTypeHint(inputGroup = InputGroup.ANY) Object second,
                       @DataTypeHint(inputGroup = InputGroup.ANY) Object... rest) {
        StringBuilder builder = new StringBuilder();
        builder.append(first);
        builder.append(second);
        for (Object field : rest) {
            builder.append(field);
        }

        return builder.toString();
    }

}