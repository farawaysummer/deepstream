package com.rui.ds.generator

import com.rui.ds.common.DataSourceConfig

open class KafkaTableGenerator(
    dsConfig: DataSourceConfig,
    tablePrefix: String = ""
) : FlinkTableGenerator(dsConfig, tablePrefix) {

    override val typeMap: Map<String, String> = mapOf(
        "DATETIME" to "TIMESTAMP(3)",
        "NUMBER" to "INTEGER",
        "VARCHAR2" to "STRING"
    )

    override val tableType: String = "kafka"

    override fun createConnectorInfo(dbName: String, tableName: String): Map<String, Any> {
        return mapOf(
            "topic" to "t_${tableName.lowercase()}",
            "properties.bootstrap.servers" to dsConfig.properties.getOrDefault("kafka.server", "192.168.4.207:9092"),
            "properties.group.id" to dsConfig.properties.getOrDefault("kafka.groupid", "k_default"),
            "scan.startup.mode" to dsConfig.properties.getOrDefault("kafka.scan.mode", "group-offsets"),
            "format" to dsConfig.properties.getOrDefault("kafka.format", "avro")
        )
    }
}