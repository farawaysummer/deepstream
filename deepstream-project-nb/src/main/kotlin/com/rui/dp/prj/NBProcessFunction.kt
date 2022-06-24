package com.rui.dp.prj

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.types.Row

class NBProcessFunction : MapFunction<Row?, Row?> {
    @Throws(Exception::class)
    override fun map(value: Row?): Row? {
        return null
    }
}