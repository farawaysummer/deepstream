package com.rui.dp.prj;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

public class SampleAsyncFunction extends RichAsyncFunction<Row, Row> {
    private static final long serialVersionUID = 1L;
    private transient AsyncClient client;

    @Override
    public void open(Configuration parameters) {
        client = new AsyncClient();
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