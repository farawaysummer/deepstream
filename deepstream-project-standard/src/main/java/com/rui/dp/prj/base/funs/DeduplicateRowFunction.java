package com.rui.dp.prj.base.funs;

import com.rui.dp.prj.base.Consts;
import com.rui.dp.prj.base.RowDesc;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

public class DeduplicateRowFunction extends KeyedProcessFunction<RowDesc, Row, Row> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeduplicateRowFunction.class);

    private ValueState<Row> valueState;
    private final List<String> keys;

    public DeduplicateRowFunction(List<String> keys) {
        this.keys = keys;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Row> valueDesc = new ValueStateDescriptor<>("rowState", Row.class);
//        valueDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
        // 完成 Keyed State 的创建。
        valueState = getRuntimeContext().getState(valueDesc);
    }

    @Override
    public void processElement(Row value, KeyedProcessFunction<RowDesc, Row, Row>.Context ctx, Collector<Row> out) throws Exception {
        Row current = valueState.value();

        if (current == null) {
            //还需要注册一个定时器
            ctx.timerService().registerEventTimeTimer(ctx.timerService().currentProcessingTime() + 1000);
            valueState.update(value);
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<RowDesc, Row, Row>.OnTimerContext ctx, Collector<Row> out) {
        Row value;
        try {
            value = valueState.value();
//            LOGGER.info("Process row value " + value);
            int[] rowFields = new int[value.getArity() - 1];
            for (int index = 0 ; index < value.getArity() - 1; index++) {
                rowFields[index] = index;
            }

            Row valueWithoutPT = Row.project(value, rowFields);

            out.collect(valueWithoutPT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        valueState.clear();
    }
}
