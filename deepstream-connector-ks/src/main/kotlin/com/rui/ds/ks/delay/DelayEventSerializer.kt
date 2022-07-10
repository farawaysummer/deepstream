package com.rui.ds.ks.delay

import com.fs.jdbc.ks.event.AvroRecordEvent
import com.fs.jdbc.ks.values.OperationType
import org.apache.kafka.common.serialization.Serializer
import java.util.*

class DelayEventSerializer : Serializer<DelayRetryRecord> {
    override fun serialize(topic: String, data: DelayRetryRecord): ByteArray {
        val columnValues = mutableMapOf<CharSequence, CharSequence>()
        data.values.forEach { (name, value) -> columnValues[name] = value }
        columnValues[DeepStreamDelay.FIELD_DEAD_LINE] = data.deadline.toString()

        val builder = AvroRecordEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setTransactionId("anyKey")
            .setOperationType(OperationType.INSERT_CODE)
            .setSchemaName("any")
            .setTableName("any")
            .setScn("0")
            .setColumnValues(columnValues)
            .setOriginalSql("")

        builder.columnTypes = emptyMap()
        builder.dateFields = emptyMap()
        builder.precisionAndScales = emptyMap()
        builder.beforeUpdatedColumns = emptyMap()
        builder.primaryKeys = emptyList()

        val avroRecordEvent = builder.build()
        return avroRecordEvent.toByteBuffer().array()
    }
}