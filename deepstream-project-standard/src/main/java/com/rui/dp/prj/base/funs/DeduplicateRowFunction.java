package com.rui.dp.prj.base.funs;

import com.rui.dp.prj.base.Consts;
import com.rui.dp.prj.base.RowDesc;
import com.rui.dp.prj.base.job.EventData;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

public class DeduplicateRowFunction extends KeyedProcessFunction<RowDesc, Row, Row> {
    private static final Logger logger = LoggerFactory.getLogger(DeduplicateRowFunction.class);
    private ValueState<Row> valueState;
    private final String jobName;
    private final EventData eventData;
    private long window = 1000;

    // counter-jobName-eventName
    private transient Counter counter;

    public DeduplicateRowFunction(String jobName, EventData event) {
        this.jobName = jobName;
        this.eventData = event;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Row> valueDesc = new ValueStateDescriptor<>("rowState", Row.class);
        valueState = getRuntimeContext().getState(valueDesc);

        counter = getRuntimeContext().getMetricGroup().counter("processRows-" + jobName + "-" + eventData.getEventName());

        String windowStr = eventData.getProperties().get("deduplicate.windows");
        if (windowStr != null) {
            try {
                this.window = Long.parseLong(windowStr);
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Override
    public void processElement(Row value, KeyedProcessFunction<RowDesc, Row, Row>.Context ctx, Collector<Row> out) throws Exception {
        // 过滤掉超过deadline的事件记录
        Set<String> fields = value.getFieldNames(true);
        if (fields != null && fields.contains(Consts.FIELD_DEAD_LINE)) {
            Long deadline = (Long) value.getField(Consts.FIELD_DEAD_LINE);
            if (deadline != null &&
                    deadline != 0L &&
                    deadline < ctx.timerService().currentProcessingTime()) {
                logger.info("Delay retry timeout, drop event {}.", value);
                return;
            }
        }

        Row current = valueState.value();

        if (current == null) {
            //还需要注册一个定时器
            ctx.timerService().registerEventTimeTimer(ctx.timerService().currentProcessingTime() + window);
            valueState.update(value);
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<RowDesc, Row, Row>.OnTimerContext ctx, Collector<Row> out) {
        Row value;
        try {
            value = valueState.value();
            int[] rowFields = new int[value.getArity() - 1];
            for (int index = 0; index < value.getArity() - 1; index++) {
                rowFields[index] = index;
            }

            Row valueWithoutPT = Row.project(value, rowFields);
            counter.inc();

            out.collect(valueWithoutPT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        valueState.clear();
    }
}
