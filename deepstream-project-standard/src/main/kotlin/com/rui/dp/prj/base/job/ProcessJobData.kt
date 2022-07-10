package com.rui.dp.prj.base.job

import com.google.common.base.Strings
import com.rui.dp.prj.base.Consts
import com.rui.ds.common.DataSourceConfig
import com.rui.ds.datasource.DatabaseSources
import org.dom4j.Element

data class ProcessJobData(
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
            conditionFields = event.fieldMapping.keys.toList(), //event.eventFields.map { it.fieldName },
            fieldMapping = event.fieldMapping,
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
    val eventName: String,
    val eventFields: List<DataField>,
    val fieldMapping: Map<String, String> = mapOf(),
    val eventType: TableType,
    val properties: Map<String, String> = mutableMapOf()
) : java.io.Serializable {
    fun toEventTableSql(): String {
        val fields = eventFields.joinToString(separator = ",") {
            "`${it.fieldName}` ${it.fieldType}"
        }
        val hasKey = eventFields.any { it.isKey }
        val primaryKeys = eventFields.filter { it.isKey }.joinToString(separator = ",") {
            "`${it.fieldName}`"
        }

        if (hasKey && !eventType.ignoreKey) {
            return """
            CREATE TABLE ${eventName.uppercase()} (
                $fields ,
                ${Consts.FIELD_PROC_TIME} as PROCTIME(),
                PRIMARY KEY (${primaryKeys}) NOT ENFORCED
            ) WITH (
                $eventType
            )
        """.trimIndent()
        } else {
            return """
            CREATE TABLE ${eventName.uppercase()} (
                $fields ,
                ${Consts.FIELD_PROC_TIME} as PROCTIME()
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
            SELECT $fields, `${Consts.FIELD_PROC_TIME}` FROM ${eventName.uppercase()}
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
    val fieldMapping: Map<String, String>,
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

abstract class ProcessJobDataLoader : JobDataLoader() {
    fun loadJobData(jobResource: String): ProcessJobData {
        val rootElement = loadResources(jobResource)

        val jobName = rootElement.attributeValue("name")

        val eventsElement = rootElement.element("events")
        val eventsData = eventsElement.elements("event").map { loadEventData(it) }

        val relatedElement = rootElement.element("relates")
        val relatedTables = loadRelatedTables(relatedElement)

        val processElement = rootElement.element("process")
        val processData = loadProcessData(processElement)

        return ProcessJobData(jobName, eventsData, relatedTables, processData)
    }

    private fun loadEventData(eventElement: Element): EventData {
        // parse event data
        val eventName = eventElement.attributeValue("name")
        val eventKeyFields = eventElement.element("eventKeys").elements("field")
        val eventKeys = eventKeyFields.map {
            DataField(
                it.attributeValue("name"),
                it.attributeValue("type"),
                it.attributeValue("isKey")?.toBoolean() ?: false,
                it.attributeValue("required")?.toBoolean() ?: false
            )
        }

        val eventFieldToRefs = eventKeyFields
            .filter { it.attributeValue("ref") != null }
            .associateBy({ it.attributeValue("name") }, { it.attributeValue("ref") })
            .toMutableMap()

        eventKeys.forEach {
            if (!eventFieldToRefs.containsKey(it.fieldName)) {
                eventFieldToRefs[it.fieldName] = it.fieldName
            }
        }

        val fieldMapping = LinkedHashMap<String, String>()
        eventFieldToRefs.forEach { (eventField, refField) -> fieldMapping[refField] = eventField }

        // eventType
        val eventTypeElement = eventElement.element("eventType")
        val eventType = loadTableType(eventTypeElement)

        val eventConfigElement = eventElement.element("eventConfig")
        val eventConfig = if (eventConfigElement != null) {
            eventConfigElement.elements("config").associateBy({it.attributeValue("name")}, {it.attributeValue("value")})
        } else {
            emptyMap()
        }

        return EventData(eventName, eventKeys, fieldMapping, eventType, eventConfig)
    }

    private fun loadRelatedTables(tableElement: Element): List<RelatedTable> {
        val tableRefs = mutableMapOf<String, List<DataField>>()
        val tables = tableElement.elements("table")
        val relatedTables = tables.map {
            val tableName = it.attributeValue("name")
            val tableRef = it.attributeValue("ref")
            if (!tableRefs.containsKey(tableRef)) {
                tableRefs[tableRef] = loadTableRef(tableRef)
            }

            val tableTypeElement = it.element("tableType")
            val tableType = loadTableType(tableTypeElement)

            // generate table create sql`
            RelatedTable(
                tableName,
                tableRefs[tableRef]!!,
                tableType
            )
        }

        return relatedTables
    }

    private fun loadProcessData(processElement: Element): ProcessData {
        val dsName = processElement.element("datasource").attributeValue("name")
        val processSqlName = processElement.element("processSQL").attributeValue("name")
        val processMasterTab = processElement.element("processSQL").attributeValue("masterTable")
        var useDynamicCondition = false
        if (!Strings.isNullOrEmpty(processMasterTab)) {
            useDynamicCondition = true
        }

        val sinkSqlName = processElement.element("sinkSQL").attributeValue("name")
        val transformsElement = processElement.element("transformJobs")
        val trans = if (transformsElement != null) {
            transformsElement.elements("transform").map { it.attributeValue("name") }
        } else {
            emptyList()
        }

        val delayQuery = processElement.element("delayQuerySecond").text.toInt()
        val resultFields = processElement.element("results").elements("field").map {
            DataField(
                it.attributeValue("name"),
                it.attributeValue("type"),
                false,
                it.attributeValue("required")?.toBoolean() ?: false
            )
        }

        return ProcessData(
            dsName = dsName,
            processSqlName = processSqlName,
            dynamicCondition = useDynamicCondition,
            masterTable = processMasterTab,
            sinkSqlName = sinkSqlName,
            dictTransforms = trans,
            queryDelay = delayQuery,
            resultFields = resultFields
        )
    }
}