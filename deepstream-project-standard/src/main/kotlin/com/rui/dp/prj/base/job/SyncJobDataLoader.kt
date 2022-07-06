package com.rui.dp.prj.base.job

import org.dom4j.Element

abstract class SyncJobDataLoader: JobDataLoader() {

    fun loadJobData(jobResource: String): DeepStreamSyncJobData {
        val rootElement = loadResources(jobResource)

        val jobName = rootElement.attributeValue("name")

        val relatedElement = rootElement.element("relates")
        val relatedTables = loadRelatedTables(relatedElement)

        val processElement = rootElement.element("process")
        val SQLs = processElement.element("syncSQLs").elements("syncSQL").map {
            it.attributeValue("name")
        }

        return DeepStreamSyncJobData(jobName, relatedTables, SQLs)
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
}