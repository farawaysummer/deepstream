package com.rui.ds.common

import com.rui.ds.StreamDataTypes
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.Table
import org.apache.flink.types.Row
import org.apache.flink.util.OutputTag

data class DataContext private constructor(
    val table: Table? = null,
    val stream: DataStream<Row>? = null
) {
    constructor(table: Table) : this(table = table, stream = null) {
        this.contextType = CONTEXT_TYPE_TABLE
    }

    constructor(stream: DataStream<Row>, types: StreamDataTypes = StreamDataTypes.INIT) : this(
        table = null,
        stream = stream
    ) {
        this.contextType = CONTEXT_TYPE_STREAM
        this.types = types
    }

    internal var contextType: String = CONTEXT_TYPE_NONE
    lateinit var types: StreamDataTypes
        internal set

    var outputTags: Map<String, OutputTag<Row>> = mapOf()

    companion object {
        const val CONTEXT_TYPE_NONE = "NONE"
        const val CONTEXT_TYPE_TABLE = "TABLE"
        const val CONTEXT_TYPE_STREAM = "STREAM"

        val EMPTY_CONTEXT = DataContext()
    }
}