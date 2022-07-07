package com.rui.dp.prj.base.funs;

import com.rui.dp.prj.base.job.QueryData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

public class AsyncDBJoinFunction extends RichAsyncFunction<Row, Row> {
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
    public void asyncInvoke(final Row input, final ResultFuture<Row> resultFuture) {
        client.query(input)
                .whenComplete(
                        (response, error) -> {
                            if (response != null) {
                                resultFuture.complete(response);
                            } else {
                                resultFuture.completeExceptionally(error);
                            }

                        });
    }


}