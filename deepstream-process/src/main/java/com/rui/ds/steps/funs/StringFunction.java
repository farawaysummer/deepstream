package com.rui.ds.steps.funs;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

public class StringFunction extends ScalarFunction {

    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
        return String.valueOf(o);
    }

}
