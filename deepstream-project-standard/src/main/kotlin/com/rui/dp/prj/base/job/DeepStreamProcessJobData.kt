package com.rui.dp.prj.base.job

import com.rui.dp.prj.base.Consts
import com.rui.ds.common.DataSourceConfig
import com.rui.ds.datasource.DatabaseSources

data class DeepStreamProcessJobData(
    val jobName: String,
    val events: List<EventData>,
    val relatedTables: List<RelatedTable>,
    val processData: ProcessData,
) : java.io.Serializable {
    private val SQLS: MutableMap<String, String> = mutableMapOf()
    private val dsConfig: MutableMap<String, DataSourceConfig> = mutableMapOf()

    fun createQueryData(event: EventData): QueryData {
        return QueryData(
            jobName = jobName,
            dsName = processData.dsName,
            businessSql = getSQL(processData.processSqlName),
            conditionFields = event.eventFields.map { it.fieldName },
            resultFields = processData.resultFields.associateBy({ it.fieldName }, { it.fieldType }),
            dsConfig = dsConfig,
            dynamicCondition = processData.dynamicCondition,
            masterTable = processData.masterTable
        )
    }

    internal fun setSQLs(sqls: Map<String, String>) {
        this.SQLS.putAll(sqls)
    }

    internal fun setDataSourceConfigs(configs: Map<String, DataSourceConfig>) {
        dsConfig.putAll(configs)
    }

    fun getSQL(name: String): String {
        return SQLS[name] ?: throw RuntimeException("未找到SQL: $name")
    }

    fun getDataSourceConfig(confName: String): DataSourceConfig {
        return dsConfig[confName] ?: throw RuntimeException("未找到数据源$confName")
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
): java.io.Serializable {
    fun toEventTableSql(): String {
        val fields = eventFields.joinToString(separator = ",") {
            "`${it.fieldName}` ${it.fieldType}"
        }
        val hasKey = eventFields.any { it.isKey }
        val primaryKeys = eventFields.filter { it.isKey }.joinToString(separator = ",") {
            "`${it.fieldName}`"
        }
        
        if (hasKey) {
            return """
            CREATE TABLE ${eventName.uppercase()} (
                $fields ,
                ${Consts.FILE_PROC_TIME} as PROCTIME(),
                PRIMARY KEY (${primaryKeys}) NOT ENFORCED
            ) WITH (
                $eventType
            )
        """.trimIndent()
        } else {
            return """
            CREATE TABLE ${eventName.uppercase()} (
                $fields ,
                ${Consts.FILE_PROC_TIME} as PROCTIME()
            ) WITH (
                $eventType
            )
        """.trimIndent()
        }

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
    val dynamicCondition: Boolean = false,
    val masterTable: String? = null,
    val sinkSqlName: String,
    val dictTransforms: List<String>,
    val queryDelay: Int = 0,
    val resultFields: List<DataField>
) : java.io.Serializable {
    fun useDictMapping(): Boolean {
        return dictTransforms.isNotEmpty()
    }
}

data class QueryData(
    val jobName: String,
    val dsName: String,
    val businessSql: String,
    val conditionFields: List<String>,
    val resultFields: Map<String, String>,
    val dsConfig: MutableMap<String, DataSourceConfig>,
    val dynamicCondition: Boolean = false,
    val masterTable: String? = null
) : java.io.Serializable {

    fun loadDataSource() {
        dsConfig.values.forEach {
            DatabaseSources.registryDataSource(it)
        }
    }
}
