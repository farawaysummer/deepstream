package com.rui.ds.generator

import com.rui.ds.common.DataSourceConfig

class JdbcCdcTableGenerator(
    dsConfig: DataSourceConfig,
    tablePrefix: String = ""
) : FlinkTableGenerator(dsConfig, tablePrefix) {
    override val typeMap: Map<String, String> = mapOf(
        "DATETIME" to "TIMESTAMP(3)",
        "NUMBER" to "INTEGER",
        "VARCHAR2" to "STRING"
    )

    override val excludeTypes: Set<String> = setOf("BLOB")

    override val tableType: String
        get() = "${dsConfig.type}-cdc"

    override fun createConnectorInfo(dbName: String, tableName: String): Map<String, Any> {
        return mapOf(
            "table-name" to tableName,
            "database-name" to dbName,
            "username" to dsConfig.username,
            "password" to dsConfig.password,
            "hostname" to dsConfig.host,
            "port" to dsConfig.port
        )
    }
}
