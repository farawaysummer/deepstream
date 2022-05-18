package com.rui.ds.steps.funs;

import com.google.common.collect.Maps;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConditionStepFunction extends ProcessFunction<Row, Row> {
    public String fieldName;
    public Map<String, OutputTag<Row>> outputTags;
    public OutputTag<Row> defaultTag;

    public ConditionStepFunction(String fieldName, Map<String, String> hops, String defaultHop) {
        this.fieldName = fieldName;
        this.defaultTag = new OutputTag<Row>(defaultHop) {};
        this.outputTags = Maps.newHashMap();

        for (Map.Entry<String, String> entry : hops.entrySet()) {
            OutputTag<Row> hopTag = new OutputTag<Row>(entry.getValue()) {};
            this.outputTags.put(entry.getKey(), hopTag);
        }
    }

    @Override
    public void processElement(Row value, ProcessFunction<Row, Row>.Context ctx, Collector<Row> out) throws Exception {
        Object fieldValue = value.getField(fieldName);

        AtomicBoolean withTag = new AtomicBoolean(false);

        outputTags.forEach(
                (condition, outputTag) -> {
                    if (match(fieldValue, condition)) {
                        ctx.output(outputTag, value);
                        withTag.set(true);
                    }
                }
        );

        if (!withTag.get()) {
            ctx.output(defaultTag, value);
        }
    }

    private boolean match(Object value, String condition) {
        if (value == null) {
            return false;
        }

        return value.toString().contains(condition);
    }
}
