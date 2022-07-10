package com.rui.dp.prj.base.funs;

import com.rui.dp.prj.base.job.QueryData;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

import java.util.Collection;
import java.util.Collections;

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
                                ProcessResult result = new ProcessResult(input, ProcessResult.ALL_PASS, response, Collections.emptyList());

                                resultFuture.complete(Collections.singletonList(result));
                            } else {
                                resultFuture.completeExceptionally(error);
                            }

                        });
    }


}