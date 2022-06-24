package com.rui.dp.prj;

import com.rui.dp.prj.function.AsyncDatabaseClient;
import com.rui.dp.prj.function.BusinessData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

public class AsyncDBJoinFunction extends RichAsyncFunction<Row, Row> {
    private static final long serialVersionUID = 1L;
    private transient AsyncDatabaseClient client;
    private final BusinessData businessData;

    public AsyncDBJoinFunction(BusinessData businessData) {
        this.businessData = businessData;
    }

    @Override
    public void open(Configuration parameters) {
        client = new AsyncDatabaseClient(businessData);
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