package com.rui.ds.generator

import com.rui.ds.common.DataSourceConfig

class KafkaTableGenerator(
    dsConfig: DataSourceConfig,
    tablePrefix: String = ""
) : FlinkTableGenerator(dsConfig, tablePrefix) {

    override val typeMap: Map<String, String> = mapOf(
        "DATETIME" to "TIMESTAMP(3)"
    )

    override val tableType: String = "kafka"

    override fun createConnectorInfo(dbName: String, tableName: String): Map<String, Any> {
        return mapOf(
            "topic" to "t_${tableName.lowercase()}",
            "properties.bootstrap.servers" to dsConfig.properties["kafka.server"]!!,
            "properties.group.id" to dsConfig.properties.getOrDefault("kafka.groupid", "k_default"),
            "scan.startup.mode" to dsConfig.properties.getOrDefault("kafka.scan.mode", "earliest-offset"),
            "format" to "avro"
        )
    }
}