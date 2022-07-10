package com.rui.dp.prj.base.funs;

import com.rui.dp.prj.base.Consts;
import com.rui.dp.prj.base.job.QueryData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class AsyncDBJoinFunction extends RichAsyncFunction<Row, ProcessResult> {
    private static final long serialVersionUID = 1L;
    private transient AsyncDatabaseClient client;
    private final QueryData queryData;
    private int delay = 1;
    // counter-jobName-eventName
    private transient Counter counter;

    public AsyncDBJoinFunction(QueryData queryData) {
        this.queryData = queryData;
    }

    public AsyncDBJoinFunction(QueryData businessData, int delayQueryTime) {
        this.queryData = businessData;
        this.delay = delayQueryTime;
    }

    @Override
    public void open(Configuration parameters) {
        counter = getRuntimeContext().getMetricGroup().counter("queryRows-" + queryData.getJobName());

        client = new AsyncDatabaseClient(queryData, delay, counter);
    }

    @Override
    public void asyncInvoke(final Row input, final ResultFuture<ProcessResult> resultFuture) {
        client.query(input)
                .whenComplete(
                        (response, error) -> {
                            if (response != null) {
                                ProcessResult result;
                                if (checkPass(response)) {
                                    result = new ProcessResult(input, ProcessResult.ALL_PASS, response);
                                } else {
                                    result = new ProcessResult(input, ProcessResult.ALL_UNFINISHED, response);
                                }

                                resultFuture.complete(Collections.singletonList(result));
                            } else {
                                resultFuture.completeExceptionally(error);
                            }

                        });
    }

    /**
     * 检查结果数据是否满足条件
     *
     * @param rows 结果数据
     * @return 是否满足条件
     */
    private boolean checkPass(Collection<Row> rows) {
        Map<String, Boolean> policies = queryData.getDelayRetryConfig().getPolicies();

        if (policies.getOrDefault(Consts.RETRY_POLICY_CHECK_EMPTY, true) && rows.isEmpty()) {
            return false;
        }

        boolean isPass = true;
        if (policies.getOrDefault(Consts.RETRY_POLICY_CHECK_REQUIRED, true)) {
            for (Row row : rows) {
                for (String field : queryData.getConditionFields()) {
                    if (row.getField(field) == null) {
                        isPass = false;
                        break;
                    }
                }
            }
        }
        
        return isPass;
    }

}