package com.rui.ds

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

data class ProcessContext(
    var env: StreamExecutionEnvironment,
    var tableEnv: StreamTableEnvironment
)