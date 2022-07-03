package com.rui.dp.prj.base

data class DeepStreamJobData(
    val jobName: String,
    val eventData: EventData,
    val relatedTables: List<String>,
    val processData: ProcessData
)

data class EventData(
    private val eventName: String,
    val eventFields: List<DataField>,
    val eventType: TableType
) {
    fun toEventTableSql(): String {
        val fields = eventFields.joinToString(separator = ",") {
            "`${it.fieldName}` ${it.fieldType}"
        }
        val primaryKeys = eventFields.filter { it.isKey }.joinToString(separator = ",") {
            "`${it.fieldName}`"
        }
        return """
            CREATE TABLE ${eventName.uppercase()} (
                $fields ,
                proctime as PROCTIME(),
                PRIMARY KEY (${primaryKeys}) NOT ENFORCED
            ) WITH (
                $eventType
            )
        """.trimIndent()
    }

    fun toEventQuerySql(): String {
        TODO()
    }
}

data class ProcessData(
    val processSqlName: String,
    val dsName: String,
    val dictTransformName: String?,
    val queryDelay: Int = 0,
    val resultFields: List<DataField>
)

data class DataField(val fieldName: String, val fieldType: String, val isKey: Boolean)

data class TableType(val type: String, val properties: Map<String, String>) {

    override fun toString(): String {
        val defStr = properties.entries.joinToString(separator = ",") {
            "'$it.key' = '${it.value}'"
        }

        return """
            'connector' = '$type',
            $defStr
        """.trimIndent()
    }
}