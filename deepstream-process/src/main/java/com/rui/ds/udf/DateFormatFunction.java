package com.rui.ds.udf;

import com.rui.ds.common.DeepStreamUDF;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@DeepStreamUDF("DP_DATE_FORMAT")
public class DateFormatFunction extends ScalarFunction {

    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object datetime) {
        if (datetime == null) {
            return "";
        }

        String result = null;
        if (datetime instanceof LocalDateTime) {
            result = ((LocalDateTime) datetime).format(DateTimeFormatter.BASIC_ISO_DATE);
        } else if (datetime instanceof LocalDate) {
            result = ((LocalDate) datetime).format(DateTimeFormatter.BASIC_ISO_DATE);
        } else {
            result = datetime.toString();
        }

        return result;
    }

}