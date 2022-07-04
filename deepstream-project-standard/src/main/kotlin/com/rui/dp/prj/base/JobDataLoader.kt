package com.rui.dp.prj.base

import com.google.common.base.Strings
import com.rui.ds.common.DataSourceConfig
import org.dom4j.Element
import java.math.BigInteger

abstract class JobDataLoader {
    abstract fun loadResources(resourceName: String): Element

    fun loadDatasource(dsResource: String): Map<String, DataSourceConfig> {
        val rootElement = loadResources(dsResource)
        val connections = rootElement.elements("connection")

        val configs = connections.map {
            parseConnection(it)
        }

        return configs.associateBy({ it.name }, { it })
    }

    fun loadSql(sqlResource: String): Map<String, String> {
        val rootElement = loadResources(sqlResource)
        val sqlElements = rootElement.elements("sql")
        return sqlElements.associateBy({ it.attributeValue("name") }, { it.text })
    }

    fun loadJobData(jobResource: String): DeepStreamProcessJobData {
        val rootElement = loadResources(jobResource)

        val jobName = rootElement.attributeValue("name")
        val eventElement = rootElement.element("event")
        val eventData = loadEventData(eventElement)

        val relatedElement = rootElement.element("relates")
        val relatedTables = loadRelatedTables(relatedElement)

        val processElement = rootElement.element("process")
        val processData = loadProcessData(processElement)

        return DeepStreamProcessJobData(jobName, eventData, relatedTables, processData)
    }

    private fun loadEventData(eventElement: Element): EventData {
        // parse event data
        val eventName = eventElement.attributeValue("name")
        val eventKeyFields = eventElement.element("eventKeys").elements("field")
        val eventKeys = eventKeyFields.map {
            DataField(
                it.attributeValue("name"),
                it.attributeValue("type"),
                it.attributeValue("isKey").toBoolean()
            )
        }

        // eventType
        val eventTypeElement = eventElement.element("eventType")
        val eventType = loadTableType(eventTypeElement)

        return EventData(eventName, eventKeys, eventType)
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

            // generate table create sql
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
                false
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

    private fun loadTableType(element: Element): TableType {
        val typeName = element.attributeValue("name")
        val typeProperties = element.elements("config")
            .associateBy({ it.attributeValue("name") }, { it.attributeValue("value") })
        return TableType(typeName, typeProperties)
    }

    private fun loadTableRef(refName: String): List<DataField> {
        val rootElement = loadResources("table-$refName")
        val fields = rootElement.elements("field").map {
            DataField(
                it.attributeValue("name"),
                it.attributeValue("type"),
                it.attributeValue("isKey")?.toBoolean() ?: false
            )
        }

        return fields
    }

    private fun parseConnection(conElement: Element): DataSourceConfig {
        val name = conElement.elementText("name")
        val host = conElement.elementText("server")
        val port = conElement.elementText("port")
        val type = conElement.elementText("type").lowercase()
        val username = conElement.elementText("username")
        val password = decryptPassword(conElement.elementText("password"))
        val database = conElement.elementText("database")

        val attributesEle = conElement.element("attributes")
        val attributes = if (attributesEle != null) {
            val elements = attributesEle.elements("attribute")
            elements.associateBy({ it.elementText("code") }, { it.elementText("attribute") })
        } else {
            emptyMap()
        }

        val subType = conElement.element("subType")?.text

        val subTypeValue = subType?.toInt() ?: DataSourceConfig.DATABASE_SUBTYPE_DEFAULT

        return DataSourceConfig(
            name = name,
            dbName = database,
            username = username,
            password = password,
            type = type,
            host = host,
            port = port.toInt(),
            properties = attributes,
            subType = subTypeValue
        )
    }

    private fun decryptPassword(encrypted: String?): String {
        if (encrypted.isNullOrEmpty()) {
            return ""
        }

        if (!encrypted.startsWith("Encrypted ")) {
            return encrypted
        }

        val encPassword = encrypted.substring("Encrypted ".length)
        val biConfuse = BigInteger(seed)
        return try {
            val biR1 = BigInteger(encPassword, 16)
            val biR0 = biR1.xor(biConfuse)
            String(biR0.toByteArray())
        } catch (e: Exception) {
            ""
        }
    }

    companion object {
        private const val seed: String = "0933910847463829827159347601486730416058"
    }
}