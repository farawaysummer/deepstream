package com.rui.dp.prj.base.funs;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ResultForwardFunction extends ProcessFunction<ProcessResult, Row> {
    public static final OutputTag<Row> outputTag = new OutputTag<Row>("delay-output") {};

    @Override
    public void processElement(ProcessResult value, ProcessFunction<ProcessResult, Row>.Context ctx, Collector<Row> out) throws Exception {
        // 已满足条件的查询结果，放到主数据流中继续处理
        if (value.getPass() <= ProcessResult.PART_PASS) {
            value.getOutput().forEach(
                    out::collect
            );
        } else {
            ctx.output(outputTag, value.getInput());
        }
        // 未满足条件（必填字段为空且未超时）的记录，对其输入事件延时后放入事件主题中

    }

}
