package com.rui.ds.udf;

import com.rui.ds.common.DeepStreamUDF;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@DeepStreamUDF("DP_DATETIME_FORMAT")
public class DatetimeFormatFunction extends ScalarFunction {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object datetime) {
        if (datetime == null) {
            return "";
        }

        String result = null;
        if (datetime instanceof LocalDateTime) {
            result = ((LocalDateTime) datetime).format(formatter);
        } else if (datetime instanceof LocalDate) {
            result = ((LocalDate) datetime).format(formatter);
        } else {
            result = datetime.toString();
        }

        return result;
    }

}