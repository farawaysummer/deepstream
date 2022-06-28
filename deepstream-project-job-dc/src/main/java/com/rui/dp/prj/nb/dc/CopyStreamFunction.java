package com.rui.dp.prj.nb.dc;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class CopyStreamFunction extends ProcessFunction<Row, Row> {
    public OutputTag<Row> defaultTag;

    public CopyStreamFunction() {
        this.defaultTag = new OutputTag<Row>("ProcessCLI") {
        };
    }

    @Override
    public void processElement(Row value, ProcessFunction<Row, Row>.Context ctx, Collector<Row> out) {
        out.collect(value);

        ctx.output(defaultTag, value);
    }
}
