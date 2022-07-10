package com.rui.dp.prj.base.funs;

import com.google.common.collect.Maps;
import com.rui.dp.prj.base.Consts;
import com.rui.dp.prj.base.job.DelayRetryConfig;
import com.rui.dp.prj.base.job.QueryData;
import com.rui.ds.ks.delay.DelayRecordPublisher;
import com.rui.ds.ks.delay.DelayRetryRecord;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

public class ResultForwardFunction extends ProcessFunction<ProcessResult, Row> {
    private static final Logger logger = LoggerFactory.getLogger(ResultForwardFunction.class);
    private final QueryData queryData;

    public ResultForwardFunction(QueryData queryData) {
        this.queryData = queryData;
    }

    @Override
    public void processElement(ProcessResult value, ProcessFunction<ProcessResult, Row>.Context ctx, Collector<Row> out) throws Exception {
        // 已满足条件的查询结果，放到主数据流中继续处理
        if (value.getPass() <= ProcessResult.PART_PASS) {
            value.getOutput().forEach(
                    out::collect
            );
        } else {
            if (queryData.getDelayRetryConfig().getEnabled()) {
                // 发送延迟重试的报文
                DelayRetryConfig config = queryData.getDelayRetryConfig();
                DelayRetryRecord retryRecord = new DelayRetryRecord();
                retryRecord.setEventTopic(config.getTarget());

                Row inputRow = value.getInput();
                Long deadline = (Long) inputRow.getField(Consts.FIELD_DEAD_LINE);
                if (deadline != null) {
                    if (deadline == 0) {
                        // 未设置过deadline的事件记录，设置为当前时间+deadline
                        retryRecord.setDeadline(System.currentTimeMillis() + Duration.ofMinutes(config.getDeadline()).toMillis());
                    } else {
                        // 维持deadline的值，以避免不超时的情况
                        retryRecord.setDeadline(deadline);
                    }
                } else {
                    retryRecord.setDeadline(System.currentTimeMillis() + Duration.ofMinutes(config.getDeadline()).toMillis());
                }

                Map<String, String> values = Maps.newLinkedHashMap();
                queryData.getConditionFields().forEach(field -> {
                    Object fieldValue = inputRow.getField(field);
                    values.put(field, String.valueOf(fieldValue));
                });
                retryRecord.setValues(values);

                DelayRecordPublisher.publish(queryData.getDelayRetryConfig().getServers(),
                        queryData.getDelayRetryConfig().getLevel(),
                        retryRecord);

                logger.info("Send retry event to delay topics: " + retryRecord);
            } else {
                logger.warn("Delay retry is disabled, processed data will be dropped.");
            }
        }

    }

}
