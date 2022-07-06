package com.rui.dp.prj.base.job

import com.google.common.base.Strings
import com.rui.ds.common.DataSourceConfig
import org.dom4j.Element
import java.math.BigInteger

abstract class ProcessJobDataLoader: JobDataLoader() {


    fun loadJobData(jobResource: String): DeepStreamProcessJobData {
        val rootElement = loadResources(jobResource)

        val jobName = rootElement.attributeValue("name")

        val eventsElement = rootElement.element("events")
        val eventsData = eventsElement.elements("event").map { loadEventData(it) }

        val relatedElement = rootElement.element("relates")
        val relatedTables = loadRelatedTables(relatedElement)

        val processElement = rootElement.element("process")
        val processData = loadProcessData(processElement)

        return DeepStreamProcessJobData(jobName, eventsData, relatedTables, processData)
    }

    private fun loadEventData(eventElement: Element): EventData {
        // parse event data
        val eventName = eventElement.attributeValue("name")
        val eventKeyFields = eventElement.element("eventKeys").elements("field")
        val eventKeys = eventKeyFields.map {
            DataField(
                it.attributeValue("name"),
                it.attributeValue("type"),
                it.attributeValue("isKey")?.toBoolean()?: false,
                it.attributeValue("from") ?: null
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
                it.attributeValue("from") ?: null
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