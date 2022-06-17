package com.rui.ds.generator

import com.rui.ds.datasource.DatabaseSources

object TableGenerator {

    fun getGenerator(dsName: String, tableType: String): FlinkTableGenerator {
        val dataSourceConfig = DatabaseSources.getDataSourceConfig(dsName)
        return when (tableType) {
            "cdc" -> JdbcCdcTableGenerator(dataSourceConfig!!)
            "dim" -> JdbcDimTableGenerator(dataSourceConfig!!)
            "kafka" -> KafkaTableGenerator(dataSourceConfig!!)
            else -> JdbcTableGenerator(dataSourceConfig!!)
        }
    }
}