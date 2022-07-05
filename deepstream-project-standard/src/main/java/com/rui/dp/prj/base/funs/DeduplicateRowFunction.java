package com.rui.dp.prj.base.funs;

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

import java.util.List;

public class DeduplicateRowFunction extends KeyedProcessFunction<RowDesc, Row, Row> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeduplicateRowFunction.class);

    private ValueState<RowDesc> valueState;
    private final List<String> keys;

    public DeduplicateRowFunction(List<String> keys) {
        this.keys = keys;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<RowDesc> valueDesc = new ValueStateDescriptor<>("rowState", RowDesc.class);
        valueDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
        // 完成 Keyed State 的创建。
        valueState = getRuntimeContext().getState(valueDesc);
    }

    @Override
    public void processElement(Row value, KeyedProcessFunction<RowDesc, Row, Row>.Context ctx, Collector<Row> out) throws Exception {
        RowDesc current = valueState.value();

        if (current == null) {
            current = RowDesc.of(value, keys);
            valueState.update(current);
            //还需要注册一个定时器
            ctx.timerService().registerEventTimeTimer(current.getTimestamp() + 3000);

            int[] rowFields = new int[value.getArity() - 1];
            for (int index = 0 ; index < value.getArity() - 1; index++) {
                rowFields[index] = index;
            }

            Row valueWithoutPT = Row.project(value, rowFields);

            out.collect(valueWithoutPT);
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<RowDesc, Row, Row>.OnTimerContext ctx, Collector<Row> out) {
        valueState.clear();
    }
}
