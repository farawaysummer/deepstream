package com.rui.dp.prj.base

import com.rui.ds.common.DataSourceConfig
import com.rui.ds.datasource.DatabaseSources

data class DeepStreamJobData(
    val jobName: String,
    val eventData: EventData,
    val relatedTables: List<RelatedTable>,
    val processData: ProcessData
) {
    private val SQLS: MutableMap<String, String> = mutableMapOf()
    private val dsConfig: MutableMap<String, DataSourceConfig> = mutableMapOf()

    val queryData: QueryData
        get() {
            return QueryData(
                dsName = processData.dsName,
                businessSql = getSQL(processData.processSqlName),
                conditionFields = eventData.eventFields.map { it.fieldName },
                resultFields = processData.resultFields.associateBy({ it.fieldName }, { it.fieldType }),
                dsConfig
            )
        }

    internal fun setSQLs(sqls: Map<String, String>) {
        this.SQLS.putAll(sqls)
    }

    internal fun setDataSourceConfigs(configs: Map<String, DataSourceConfig>) {
        dsConfig.putAll(configs)
    }

    fun getSQL(name: String): String {
        return SQLS[name]?: throw RuntimeException("未找到SQL: $name")
    }

    fun getDataSourceConfig(confName: String): DataSourceConfig {
        return dsConfig[confName]?: throw RuntimeException("未找到数据源$confName")
    }

    fun loadDataSource() {
        dsConfig.values.forEach {
            DatabaseSources.registryDataSource(it)
        }
    }
}

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
                ${Consts.FILE_PROC_TIME} as PROCTIME(),
                PRIMARY KEY (${primaryKeys}) NOT ENFORCED
            ) WITH (
                $eventType
            )
        """.trimIndent()
    }

    fun toEventQuerySql(): String {
        val fields = eventFields.joinToString(separator = ",") {
            "`${it.fieldName}`"
        }
        return """
            SELECT $fields, `${Consts.FILE_PROC_TIME}` FROM ${eventName.uppercase()}
        """.trimIndent()
    }
}

data class ProcessData(
    val dsName: String,
    val processSqlName: String,
    val sinkSqlName: String,
    val dictTransforms: List<String>,
    val queryDelay: Int = 0,
    val resultFields: List<DataField>
) {
    val useDictMapping: Boolean
        get() {
            return dictTransforms.isNotEmpty()
        }
}

data class DataField(val fieldName: String, val fieldType: String, val isKey: Boolean)

data class RelatedTable(val tableName: String, val tableFields: List<DataField>, val tableType: TableType) {
    fun toTableSql(): String {
        val fields = tableFields.joinToString(separator = ",") {
            "`${it.fieldName}` ${it.fieldType}"
        }

        val primaryKeys = tableFields.filter { it.isKey }.joinToString(separator = ",") {
            "`${it.fieldName}`"
        }

        return """
            CREATE TABLE $tableName (
                $fields ,
                PRIMARY KEY (${primaryKeys}) NOT ENFORCED
            ) WITH (
                $tableType
            )
        """.trimIndent()
    }
}

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

data class QueryData(
    val dsName: String,
    val businessSql: String,
    val conditionFields: List<String>,
    val resultFields: Map<String, String>,
    val dsConfig: MutableMap<String, DataSourceConfig>
): java.io.Serializable {

    fun loadDataSource() {
        dsConfig.values.forEach {
            DatabaseSources.registryDataSource(it)
        }
    }
}
