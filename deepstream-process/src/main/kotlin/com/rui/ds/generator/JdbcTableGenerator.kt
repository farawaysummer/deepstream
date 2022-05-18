package com.rui.ds.generator

import com.rui.ds.common.DataSourceConfig

open class JdbcTableGenerator(
    dsConfig: DataSourceConfig,
    tablePrefix: String = ""
) : FlinkTableGenerator(dsConfig, tablePrefix) {

    override val typeMap: Map<String, String> = mapOf(
        "DATETIME" to "TIMESTAMP(3)"
    )

    override val tableType: String = "jdbc"

    override fun createConnectorInfo(dbName: String, tableName: String): Map<String, Any> {
        return mapOf(
            "url" to dsConfig.connectString,
            "table-name" to tableName,
            "username" to dsConfig.username,
            "password" to dsConfig.password,
            "driver" to dsConfig.driverClassName
        )
    }
}