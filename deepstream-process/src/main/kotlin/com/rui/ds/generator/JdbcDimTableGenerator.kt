package com.rui.ds.generator

import com.rui.ds.common.DataSourceConfig

class JdbcDimTableGenerator(
    dsConfig: DataSourceConfig,
    tablePrefix: String = ""
): JdbcTableGenerator(dsConfig, tablePrefix) {

    override fun createConnectorInfo(dbName: String, tableName: String): Map<String, Any> {
        val map =  super.createConnectorInfo(dbName, tableName)

        return map.toMutableMap()
            .plus("lookup.cache.max-rows" to "5000")
            .plus("lookup.cache.ttl" to "5min")
            .toMap()
    }
}